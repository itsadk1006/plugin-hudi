package io.kestra.plugin.hudi;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public abstract class HudiRecordUtils {

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> parseRawRecords(RunContext runContext, Object fromObj) throws Exception {
        if (fromObj == null) {
            return Collections.emptyList();
        }

        if (fromObj instanceof List<?> list) {
            var records = new ArrayList<Map<String, Object>>(list.size());
            for (var item : list) {
                if (item instanceof Map<?, ?> map) {
                    records.add((Map<String, Object>) map);
                } else {
                    var converted = JacksonMapper.ofJson().convertValue(item, new TypeReference<Map<String, Object>>() {});
                    records.add(converted);
                }
            }
            return records;
        }

        if (fromObj instanceof Map<?, ?> map) {
            return List.of((Map<String, Object>) map);
        }

        var str = fromObj.toString();
        if (str.startsWith("kestra://")) {
            var records = new ArrayList<Map<String, Object>>();
            var uri = URI.create(str);
            try (var is = runContext.storage().getFile(uri);
                 var reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
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

        try {
            return JacksonMapper.ofJson().readValue(str, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            var single = JacksonMapper.ofJson().readValue(str, new TypeReference<Map<String, Object>>() {});
            return List.of(single);
        }
    }

    public static Schema inferOrGetSchema(List<Map<String, Object>> records, String recordKeyField, String precombineField, String userSchema) {
        if (userSchema != null && !userSchema.isBlank()) {
            return new Schema.Parser().parse(userSchema);
        }

        var fieldNames = new LinkedHashSet<String>();
        if (recordKeyField != null && !recordKeyField.isBlank()) {
            fieldNames.add(recordKeyField);
        }
        if (precombineField != null && !precombineField.isBlank()) {
            fieldNames.add(precombineField);
        }
        for (var r : records) {
            fieldNames.addAll(r.keySet());
        }

        if (fieldNames.isEmpty()) {
            fieldNames.add("id");
        }

        var sb = new StringBuilder();
        sb.append("{\"type\":\"record\",\"name\":\"hudi_record\",\"namespace\":\"io.kestra.plugin.hudi\",\"fields\":[");
        var index = 0;
        for (var fieldName : fieldNames) {
            if (index++ > 0) {
                sb.append(",");
            }
            Object sample = null;
            for (var r : records) {
                if (r.get(fieldName) != null) {
                    sample = r.get(fieldName);
                    break;
                }
            }
            var avroType = "string";
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
        var result = new ArrayList<HoodieRecord<HoodieAvroPayload>>(records.size());
        var index = -1;
        for (var record : records) {
            index++;
            GenericRecord genericRecord = new GenericData.Record(schema);
            for (var field : schema.getFields()) {
                var val = record.get(field.name());
                if (val != null) {
                    var type = field.schema().getType();
                    if (type == Schema.Type.UNION) {
                        for (var s : field.schema().getTypes()) {
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

            var key = requireKey(record, recordKeyField, index);
            var partition = (partitionField != null && record.get(partitionField) != null) ? String.valueOf(record.get(partitionField)) : "";

            var hoodieKey = new HoodieKey(key, partition);
            var payload = new HoodieAvroPayload(Option.of(genericRecord));
            result.add(new HoodieAvroRecord<>(hoodieKey, payload));
        }
        return result;
    }

    private static String requireKey(Map<?, ?> record, String recordKeyField, int index) {
        var kv = record.get(recordKeyField);
        if (kv == null) {
            throw new IllegalArgumentException("Record at index " + index + " has no '" + recordKeyField + "' field, set recordKeyField to an existing field");
        }
        return String.valueOf(kv);
    }

    private static HoodieKey toDeleteKey(Map<?, ?> record, String recordKeyField, String partitionField, int index) {
        var partition = partitionField != null && record.get(partitionField) != null ? String.valueOf(record.get(partitionField)) : "";
        return new HoodieKey(requireKey(record, recordKeyField, index), partition);
    }

    public static List<HoodieKey> parseDeleteKeys(RunContext runContext, Object fromObj, String recordKeyField, String partitionField) throws Exception {
        if (fromObj == null) {
            return Collections.emptyList();
        }

        var keys = new ArrayList<HoodieKey>();
        if (fromObj instanceof List<?> list) {
            for (var i = 0; i < list.size(); i++) {
                var item = list.get(i);
                if (item instanceof Map<?, ?> map) {
                    keys.add(toDeleteKey(map, recordKeyField, partitionField, i));
                } else {
                    keys.add(new HoodieKey(String.valueOf(item), ""));
                }
            }
            return keys;
        }

        if (fromObj instanceof Map<?, ?> map) {
            return List.of(toDeleteKey(map, recordKeyField, partitionField, 0));
        }

        var str = fromObj.toString();
        if (str.startsWith("kestra://")) {
            var records = parseRawRecords(runContext, str);
            for (var i = 0; i < records.size(); i++) {
                keys.add(toDeleteKey(records.get(i), recordKeyField, partitionField, i));
            }
            return keys;
        }

        // Kestra renders a list or map property as its JSON text
        var trimmed = str.strip();
        if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
            Object parsed;
            try {
                parsed = JacksonMapper.ofJson().readValue(trimmed, Object.class);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Property 'from' is not valid JSON: " + e.getOriginalMessage(), e);
            }
            return parseDeleteKeys(runContext, parsed, recordKeyField, partitionField);
        }

        keys.add(new HoodieKey(str, ""));
        return keys;
    }
}
