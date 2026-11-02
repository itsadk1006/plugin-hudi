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

import io.kestra.core.runners.RunContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            for (var baseFile : view.getLatestBaseFiles("").toList()) {
                try (var reader = AvroParquetReader.<GenericRecord>builder(new Path(baseFile.getPath())).withConf(hadoopConf).build()) {
                    GenericRecord record;
                    while ((record = reader.read()) != null) {
                        records.add(record);
                    }
                }
            }
        }
        return records;
    }

    static String field(GenericRecord record, String name) {
        return String.valueOf(record.get(name));
    }
}
