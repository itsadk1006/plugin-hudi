package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHudiWrite extends AbstractHudiConnection {

    @Schema(
        title = "Record key field",
        description = """
            Field name to use as the record key for Hudi records.
            """
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<String> recordKeyField;

    @Schema(
        title = "Partition path field",
        description = """
            Field name to use as partition path. Defaults to empty for non-partitioned tables.
            """
    )
    @PluginProperty(group = "processing")
    private Property<String> partitionPathField;

    @Schema(
        title = "Avro schema",
        description = """
            Optional Avro schema JSON string. If omitted, schema is inferred from records.
            """
    )
    @PluginProperty(group = "processing")
    private Property<String> schema;

    @Schema(
        title = "Source data",
        description = """
            Data to write. Can be an inline list of maps or a Kestra internal storage URI (kestra://...).
            """
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<Object> from;

    protected abstract Property<String> getPrecombineField();

    protected abstract String operationName();

    protected abstract List<WriteStatus> write(HoodieJavaWriteClient<HoodieAvroPayload> client, List<HoodieRecord<HoodieAvroPayload>> records, String instantTime);

    protected abstract long writtenRecords(HoodieWriteStat stat);

    protected Output runWrite(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rBasePath = AbstractHudiConnection.resolveBasePath(runContext, this.basePath);
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));
        var rRecordKey = runContext.render(this.recordKeyField).as(String.class).orElseThrow(() -> new IllegalArgumentException("recordKeyField is required"));
        var rPartition = this.partitionPathField != null ? runContext.render(this.partitionPathField).as(String.class).orElse(null) : null;
        var rPrecombine = getPrecombineField() != null ? runContext.render(getPrecombineField()).as(String.class).orElse(null) : null;
        var rSchema = this.schema != null ? runContext.render(this.schema).as(String.class).orElse(null) : null;
        var rFrom = runContext.render(this.from).as(Object.class).orElse(null);

        logger.info("Writing ({}) records into Hudi table '{}' at '{}'", operationName(), rTableName, rBasePath);

        var rawRecords = HudiRecordUtils.parseRawRecords(runContext, rFrom);
        if (rawRecords.isEmpty()) {
            logger.warn("No records to {} into Hudi table '{}'", operationName(), rTableName);
            return Output.builder()
                .recordCount(0L)
                .affectedPartitions(Collections.emptyList())
                .build();
        }

        var avroSchema = HudiRecordUtils.inferOrGetSchema(rawRecords, rRecordKey, rPrecombine, rSchema);
        var hoodieRecords = HudiRecordUtils.toHoodieRecords(rawRecords, avroSchema, rRecordKey, rPartition);

        getOrInitMetaClient(runContext, rBasePath, rTableName, HoodieTableType.COPY_ON_WRITE);

        var props = new Properties();
        props.setProperty("hoodie.datasource.write.recordkey.field", rRecordKey);
        if (rPartition != null) {
            props.setProperty("hoodie.datasource.write.partitionpath.field", rPartition);
        }
        if (rPrecombine != null) {
            props.setProperty("hoodie.datasource.write.precombine.field", rPrecombine);
        }

        var writeConfig = baseWriteConfigBuilder(runContext, rBasePath, rTableName)
            .withSchema(avroSchema.toString())
            .withProperties(props)
            .build();

        try (var client = createJavaWriteClient(runContext, writeConfig)) {
            var instantTime = client.startCommit();
            var statuses = write(client, hoodieRecords, instantTime);
            commitOrFail(client, instantTime, statuses, rTableName);

            var totalRecords = 0L;
            var affectedPartitions = new LinkedHashSet<String>();
            for (var status : statuses) {
                totalRecords += writtenRecords(status.getStat());
                if (status.getPartitionPath() != null) {
                    affectedPartitions.add(status.getPartitionPath());
                }
            }

            logger.info("Successfully wrote ({}) {} records at instant {}", operationName(), totalRecords, instantTime);
            return Output.builder()
                .recordCount(totalRecords)
                .commitInstantTime(instantTime)
                .affectedPartitions(new ArrayList<>(affectedPartitions))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of records written",
            description = """
                Total number of records written by the operation, as reported by the Hudi write statuses.
                """
        )
        private Long recordCount;

        @Schema(
            title = "Commit instant timestamp",
            description = """
                The instant timestamp generated for this commit on the Hudi active timeline.
                """
        )
        private String commitInstantTime;

        @Schema(
            title = "Affected partition paths",
            description = """
                List of partition paths modified during the operation.
                """
        )
        private List<String> affectedPartitions;
    }
}
