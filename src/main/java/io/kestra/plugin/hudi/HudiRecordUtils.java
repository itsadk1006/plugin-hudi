package io.kestra.plugin.hudi;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class HudiRecordUtils {

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> parseRawRecords(RunContext runContext, Object fromObj) throws Exception {
        if (fromObj == null) {
            return Collections.emptyList();
        }

        if (fromObj instanceof List<?> list) {
            List<Map<String, Object>> records = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    records.add((Map<String, Object>) map);
                } else {
                    Map<String, Object> converted = JacksonMapper.ofJson().convertValue(item, new TypeReference<>() {});
                    records.add(converted);
                }
            }
            return records;
        }

        if (fromObj instanceof Map<?, ?> map) {
            return List.of((Map<String, Object>) map);
        }

        String str = fromObj.toString();
        if (str.startsWith("kestra://")) {
            List<Map<String, Object>> records = new ArrayList<>();
            URI uri = URI.create(str);
            try (InputStream is = runContext.storage().getFile(uri);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        Map<String, Object> record = JacksonMapper.ofJson().readValue(line, new TypeReference<>() {});
                        records.add(record);
                    }
                }
            }
            return records;
        }

        // Try parsing string as JSON array or single JSON object
        try {
            return JacksonMapper.ofJson().readValue(str, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            Map<String, Object> single = JacksonMapper.ofJson().readValue(str, new TypeReference<>() {});
            return List.of(single);
        }
    }

    public static Schema inferOrGetSchema(List<Map<String, Object>> records, String recordKeyField, String precombineField, String userSchema) {
        if (userSchema != null && !userSchema.isBlank()) {
            return new Schema.Parser().parse(userSchema);
        }

        Set<String> fieldNames = new LinkedHashSet<>();
        if (recordKeyField != null && !recordKeyField.isBlank()) {
            fieldNames.add(recordKeyField);
        }
        if (precombineField != null && !precombineField.isBlank()) {
            fieldNames.add(precombineField);
        }
        for (Map<String, Object> r : records) {
            fieldNames.addAll(r.keySet());
        }

        if (fieldNames.isEmpty()) {
            fieldNames.add("id");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\",\"name\":\"hudi_record\",\"namespace\":\"io.kestra.plugin.hudi\",\"fields\":[");
        int index = 0;
        for (String fieldName : fieldNames) {
            if (index++ > 0) {
                sb.append(",");
            }
            Object sample = null;
            for (Map<String, Object> r : records) {
                if (r.get(fieldName) != null) {
                    sample = r.get(fieldName);
                    break;
                }
            }
            String avroType = "string";
            if (sample instanceof Integer || sample instanceof Long) {
                avroType = "long";
            } else if (sample instanceof Float || sample instanceof Double) {
                avroType = "double";
            } else if (sample instanceof Boolean) {
                avroType = "boolean";
            }
            sb.append(String.format("{\"name\":\"%s\",\"type\":[\"null\",\"%s\"],\"default\":null}", fieldName, avroType));
        }
        sb.append("]}");

        return new Schema.Parser().parse(sb.toString());
    }

    public static List<HoodieRecord<HoodieAvroPayload>> toHoodieRecords(
        List<Map<String, Object>> records,
        Schema schema,
        String recordKeyField,
        String partitionField
    ) {
        List<HoodieRecord<HoodieAvroPayload>> result = new ArrayList<>(records.size());
        for (Map<String, Object> record : records) {
            GenericRecord genericRecord = new GenericData.Record(schema);
            for (Schema.Field field : schema.getFields()) {
                Object val = record.get(field.name());
                if (val != null) {
                    Schema.Type type = field.schema().getType();
                    if (type == Schema.Type.UNION) {
                        for (Schema s : field.schema().getTypes()) {
                            if (s.getType() != Schema.Type.NULL) {
                                type = s.getType();
                                break;
                            }
                        }
                    }

                    if (type == Schema.Type.LONG) {
                        if (val instanceof Number n) {
                            genericRecord.put(field.name(), n.longValue());
                        } else {
                            genericRecord.put(field.name(), Long.parseLong(val.toString()));
                        }
                    } else if (type == Schema.Type.DOUBLE) {
                        if (val instanceof Number n) {
                            genericRecord.put(field.name(), n.doubleValue());
                        } else {
                            genericRecord.put(field.name(), Double.parseDouble(val.toString()));
                        }
                    } else if (type == Schema.Type.BOOLEAN) {
                        if (val instanceof Boolean b) {
                            genericRecord.put(field.name(), b);
                        } else {
                            genericRecord.put(field.name(), Boolean.parseBoolean(val.toString()));
                        }
                    } else {
                        genericRecord.put(field.name(), val.toString());
                    }
                }
            }

            Object kv = record.get(recordKeyField);
            String key = kv != null ? String.valueOf(kv) : UUID.randomUUID().toString();
            String partition = (partitionField != null && record.get(partitionField) != null) ? String.valueOf(record.get(partitionField)) : "";

            HoodieKey hoodieKey = new HoodieKey(key, partition);
            HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(genericRecord));
            result.add(new HoodieAvroRecord<>(hoodieKey, payload));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static List<HoodieKey> parseDeleteKeys(RunContext runContext, Object fromObj, String recordKeyField, String partitionField) throws Exception {
        if (fromObj == null) {
            return Collections.emptyList();
        }

        List<HoodieKey> keys = new ArrayList<>();
        if (fromObj instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    Object kv = map.get(recordKeyField);
                    String key = kv != null ? String.valueOf(kv) : "";
                    String partition = partitionField != null && map.get(partitionField) != null ? String.valueOf(map.get(partitionField)) : "";
                    keys.add(new HoodieKey(key, partition));
                } else {
                    keys.add(new HoodieKey(String.valueOf(item), ""));
                }
            }
            return keys;
        }

        if (fromObj instanceof Map<?, ?> map) {
            Object kv = map.get(recordKeyField);
            String key = kv != null ? String.valueOf(kv) : "";
            String partition = partitionField != null && map.get(partitionField) != null ? String.valueOf(map.get(partitionField)) : "";
            return List.of(new HoodieKey(key, partition));
        }

        String str = fromObj.toString();
        if (str.startsWith("kestra://")) {
            List<Map<String, Object>> records = parseRawRecords(runContext, str);
            for (Map<String, Object> map : records) {
                Object kv = map.get(recordKeyField);
                String key = kv != null ? String.valueOf(kv) : "";
                String partition = partitionField != null && map.get(partitionField) != null ? String.valueOf(map.get(partitionField)) : "";
                keys.add(new HoodieKey(key, partition));
            }
            return keys;
        }

        keys.add(new HoodieKey(str, ""));
        return keys;
    }
}
