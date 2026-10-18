package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
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
import org.apache.hudi.config.HoodieWriteConfig;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Upsert records into an Apache Hudi table",
            code = """
                id: hudi_upsert_flow
                namespace: company.team

                tasks:
                  - id: upsert_records
                    type: io.kestra.plugin.hudi.Upsert
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                    recordKeyField: order_id
                    precombineField: updated_at
                    from:
                      - order_id: 1
                        updated_at: 1672531199000
                        status: COMPLETED
                """
        )
    }
)
@Schema(
    title = "Upsert data into an Apache Hudi table",
    description = "Upsert records into an Apache Hudi table using HoodieJavaWriteClient."
)
public class Upsert extends AbstractHudiConnection implements RunnableTask<Upsert.Output> {

    @Schema(
        title = "Record key field",
        description = "Field name to use as the record key for Hudi records."
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<String> recordKeyField;

    @Schema(
        title = "Partition path field",
        description = "Field name to use as partition path. Defaults to empty for non-partitioned tables."
    )
    @PluginProperty(group = "processing")
    private Property<String> partitionPathField;

    @Schema(
        title = "Precombine field",
        description = "Field name to determine the latest record when deduplicating."
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<String> precombineField;

    @Schema(
        title = "Avro schema",
        description = "Optional Avro schema JSON string. If omitted, schema is inferred from records."
    )
    @PluginProperty(group = "processing")
    private Property<String> schema;

    @Schema(
        title = "Source data",
        description = "Data to upsert. Can be an inline list of maps or a Kestra internal storage URI (kestra://...)."
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<Object> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedBasePath = runContext.render(this.basePath).as(String.class).orElseThrow();
        String renderedTableName = runContext.render(this.tableName).as(String.class).orElseThrow();
        String renderedRecordKey = runContext.render(this.recordKeyField).as(String.class).orElseThrow();
        String renderedPartition = this.partitionPathField != null ? runContext.render(this.partitionPathField).as(String.class).orElse(null) : null;
        String renderedPrecombine = runContext.render(this.precombineField).as(String.class).orElseThrow();
        String renderedSchema = this.schema != null ? runContext.render(this.schema).as(String.class).orElse(null) : null;
        Object renderedFrom = runContext.render(this.from).as(Object.class).orElse(null);

        logger.info("Upserting records into Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        List<Map<String, Object>> rawRecords = HudiRecordUtils.parseRawRecords(runContext, renderedFrom);
        if (rawRecords.isEmpty()) {
            logger.warn("No records to upsert into Hudi table '{}'", renderedTableName);
            return Output.builder()
                .recordCount(0L)
                .commitInstantTime(null)
                .affectedPartitions(Collections.emptyList())
                .build();
        }

        org.apache.avro.Schema avroSchema = HudiRecordUtils.inferOrGetSchema(rawRecords, renderedRecordKey, renderedPrecombine, renderedSchema);
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = HudiRecordUtils.toHoodieRecords(rawRecords, avroSchema, renderedRecordKey, renderedPartition);

        getOrInitMetaClient(runContext, renderedBasePath, renderedTableName, HoodieTableType.COPY_ON_WRITE);

        Properties props = new Properties();
        props.setProperty("hoodie.datasource.write.recordkey.field", renderedRecordKey);
        if (renderedPartition != null) {
            props.setProperty("hoodie.datasource.write.partitionpath.field", renderedPartition);
        }
        props.setProperty("hoodie.datasource.write.precombine.field", renderedPrecombine);

        HoodieWriteConfig writeConfig = baseWriteConfigBuilder(runContext, renderedBasePath, renderedTableName)
            .withSchema(avroSchema.toString())
            .withProperties(props)
            .build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createJavaWriteClient(runContext, writeConfig)) {
            String instantTime = client.startCommit();
            List<WriteStatus> statuses = client.upsert(hoodieRecords, instantTime);
            client.commit(instantTime, statuses);

            long totalRecords = 0L;
            Set<String> affectedPartitions = new LinkedHashSet<>();
            for (WriteStatus status : statuses) {
                totalRecords += status.getStat().getNumInserts() + status.getStat().getNumUpdateWrites();
                if (status.getPartitionPath() != null) {
                    affectedPartitions.add(status.getPartitionPath());
                }
            }
            if (totalRecords == 0L) {
                totalRecords = hoodieRecords.size();
            }

            logger.info("Successfully upserted {} records at instant {}", totalRecords, instantTime);
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
            title = "Number of records upserted",
            description = "Total number of records affected by the upsert operation."
        )
        private Long recordCount;

        @Schema(
            title = "Commit instant timestamp",
            description = "The instant timestamp generated for this commit on the Hudi active timeline."
        )
        private String commitInstantTime;

        @Schema(
            title = "Affected partition paths",
            description = "List of partition paths modified during the upsert operation."
        )
        private List<String> affectedPartitions;
    }
}
