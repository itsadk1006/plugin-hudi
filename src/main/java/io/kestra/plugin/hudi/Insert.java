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
            title = "Insert records into an Apache Hudi table",
            code = """
                id: hudi_insert_flow
                namespace: company.team

                tasks:
                  - id: insert_records
                    type: io.kestra.plugin.hudi.Insert
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                    recordKeyField: order_id
                    from:
                      - order_id: 101
                        customer: John Doe
                        amount: 150.50
                """
        )
    }
)
@Schema(
    title = "Insert data into an Apache Hudi table",
    description = "Insert records into an Apache Hudi table using HoodieJavaWriteClient."
)
public class Insert extends AbstractHudiConnection implements RunnableTask<Insert.Output> {

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
    private Property<String> precombineField;

    @Schema(
        title = "Avro schema",
        description = "Optional Avro schema JSON string. If omitted, schema is inferred from records."
    )
    @PluginProperty(group = "processing")
    private Property<String> schema;

    @Schema(
        title = "Source data",
        description = "Data to insert. Can be an inline list of maps or a Kestra internal storage URI (kestra://...)."
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
        String renderedPrecombine = this.precombineField != null ? runContext.render(this.precombineField).as(String.class).orElse(null) : null;
        String renderedSchema = this.schema != null ? runContext.render(this.schema).as(String.class).orElse(null) : null;
        Object renderedFrom = runContext.render(this.from).as(Object.class).orElse(null);

        logger.info("Inserting records into Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        List<Map<String, Object>> rawRecords = HudiRecordUtils.parseRawRecords(runContext, renderedFrom);
        if (rawRecords.isEmpty()) {
            logger.warn("No records to insert into Hudi table '{}'", renderedTableName);
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
        if (renderedPrecombine != null) {
            props.setProperty("hoodie.datasource.write.precombine.field", renderedPrecombine);
        }

        HoodieWriteConfig writeConfig = baseWriteConfigBuilder(runContext, renderedBasePath, renderedTableName)
            .withSchema(avroSchema.toString())
            .withProperties(props)
            .build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createJavaWriteClient(runContext, writeConfig)) {
            String instantTime = client.startCommit();
            List<WriteStatus> statuses = client.insert(hoodieRecords, instantTime);
            client.commit(instantTime, statuses);

            long totalRecords = 0L;
            Set<String> affectedPartitions = new LinkedHashSet<>();
            for (WriteStatus status : statuses) {
                totalRecords += status.getStat().getNumInserts();
                if (status.getPartitionPath() != null) {
                    affectedPartitions.add(status.getPartitionPath());
                }
            }
            if (totalRecords == 0L) {
                totalRecords = hoodieRecords.size();
            }

            logger.info("Successfully inserted {} records at instant {}", totalRecords, instantTime);
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
            title = "Number of records inserted",
            description = "Total number of records inserted into the table."
        )
        private Long recordCount;

        @Schema(
            title = "Commit instant timestamp",
            description = "The instant timestamp generated for this commit on the Hudi active timeline."
        )
        private String commitInstantTime;

        @Schema(
            title = "Affected partition paths",
            description = "List of partition paths modified during the insert operation."
        )
        private List<String> affectedPartitions;
    }
}
