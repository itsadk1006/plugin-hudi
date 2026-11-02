package io.kestra.plugin.hudi;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class HudiTestUtils {

    private HudiTestUtils() {
    }

    /** Table location inside the run working directory, the only place local tables are allowed. */
    static java.nio.file.Path tablePath(RunContext runContext) {
        return runContext.workingDir().path().resolve("table");
    }

    static String tableUri(RunContext runContext) {
        return tablePath(runContext).toUri().toString();
    }

    static List<GenericRecord> readLatestRecords(String basePath) throws IOException {
        return readLatestRecords(basePath, "");
    }

    static List<GenericRecord> readLatestRecords(String basePath, String... partitions) throws IOException {
        var hadoopConf = new Configuration();
        var metaClient = HoodieTableMetaClient.builder()
            .setConf(new HadoopStorageConfiguration(hadoopConf))
            .setBasePath(basePath)
            .build();

        var records = new ArrayList<GenericRecord>();
        try (var view = FileSystemViewManager.createInMemoryFileSystemView(
            new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf)),
            metaClient,
            HoodieMetadataConfig.newBuilder().enable(false).build()
        )) {
            for (var partition : partitions) {
                for (var baseFile : view.getLatestBaseFiles(partition).toList()) {
                    try (var reader = AvroParquetReader.<GenericRecord>builder(new Path(baseFile.getPath())).withConf(hadoopConf).build()) {
                        GenericRecord record;
                        while ((record = reader.read()) != null) {
                            records.add(record);
                        }
                    }
                }
            }
        }
        return records;
    }

    static String field(GenericRecord record, String name) {
        return String.valueOf(record.get(name));
    }

    /** Creates an empty table of the given type with the Hudi client, the plugin itself only creates copy on write tables. */
    static void initTable(String basePath, String tableName, HoodieTableType tableType) throws IOException {
        HoodieTableMetaClient.newTableBuilder()
            .setTableType(tableType)
            .setTableName(tableName)
            .setBaseFileFormat(HoodieFileFormat.PARQUET.name())
            .initTable(new HadoopStorageConfiguration(new Configuration()), basePath);
    }

    /** One commit with the given rows, each commit creating its own file when the small file limit is 0. */
    static Upsert.Output upsert(RunContext runContext, String basePath, String tableName, List<Map<String, Object>> rows) throws Exception {
        return Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue(tableName))
            .recordKeyField(Property.ofValue("id"))
            .precombineField(Property.ofValue("ts"))
            .configuration(Property.ofValue(Map.of("hoodie.parquet.small.file.limit", "0")))
            .from(Property.ofValue(rows))
            .build()
            .run(runContext);
    }

    static HoodieTableMetaClient metaClient(String basePath) {
        return HoodieTableMetaClient.builder()
            .setConf(new HadoopStorageConfiguration(new Configuration()))
            .setBasePath(basePath)
            .build();
    }

    static List<String> latestBaseFileInstants(String basePath) throws Exception {
        var hadoopConf = new Configuration();
        try (var view = FileSystemViewManager.createInMemoryFileSystemView(
            new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf)),
            metaClient(basePath),
            HoodieMetadataConfig.newBuilder().enable(false).build()
        )) {
            return view.getLatestBaseFiles("").map(f -> f.getCommitTime()).toList();
        }
    }
}
