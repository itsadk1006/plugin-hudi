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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
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
            title = "Delete records from an Apache Hudi table",
            code = """
                id: hudi_delete_flow
                namespace: company.team

                tasks:
                  - id: delete_records
                    type: io.kestra.plugin.hudi.Delete
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                    recordKeyField: order_id
                    from:
                      - order_id: 101
                """
        )
    }
)
@Schema(
    title = "Delete data from an Apache Hudi table",
    description = "Delete records from an Apache Hudi table using HoodieJavaWriteClient."
)
public class Delete extends AbstractHudiConnection implements RunnableTask<Delete.Output> {

    @Schema(
        title = "Record key field",
        description = "Field name to extract the record key from incoming records."
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<String> recordKeyField;

    @Schema(
        title = "Partition path field",
        description = "Field name to extract the partition path. Defaults to empty for non-partitioned tables."
    )
    @PluginProperty(group = "processing")
    private Property<String> partitionPathField;

    @Schema(
        title = "Source keys or records",
        description = "Keys or records to delete. Can be a list of maps, strings, or a Kestra internal storage URI (kestra://...)."
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
        Object renderedFrom = runContext.render(this.from).as(Object.class).orElse(null);

        logger.info("Deleting records from Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        List<HoodieKey> keys = HudiRecordUtils.parseDeleteKeys(runContext, renderedFrom, renderedRecordKey, renderedPartition);
        if (keys.isEmpty()) {
            logger.warn("No keys to delete from Hudi table '{}'", renderedTableName);
            return Output.builder()
                .recordCount(0L)
                .commitInstantTime(null)
                .affectedPartitions(Collections.emptyList())
                .build();
        }

        getOrInitMetaClient(runContext, renderedBasePath, renderedTableName, HoodieTableType.COPY_ON_WRITE);

        Properties props = new Properties();
        props.setProperty("hoodie.datasource.write.recordkey.field", renderedRecordKey);
        if (renderedPartition != null) {
            props.setProperty("hoodie.datasource.write.partitionpath.field", renderedPartition);
        }

        HoodieWriteConfig writeConfig = baseWriteConfigBuilder(runContext, renderedBasePath, renderedTableName)
            .withProperties(props)
            .build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createJavaWriteClient(runContext, writeConfig)) {
            String instantTime = client.startCommit();
            List<WriteStatus> statuses = client.delete(keys, instantTime);
            client.commit(instantTime, statuses);

            long totalDeleted = keys.size();
            Set<String> affectedPartitions = new LinkedHashSet<>();
            for (HoodieKey k : keys) {
                if (k.getPartitionPath() != null && !k.getPartitionPath().isEmpty()) {
                    affectedPartitions.add(k.getPartitionPath());
                }
            }

            logger.info("Successfully deleted {} records at instant {}", totalDeleted, instantTime);
            return Output.builder()
                .recordCount(totalDeleted)
                .commitInstantTime(instantTime)
                .affectedPartitions(new ArrayList<>(affectedPartitions))
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of records deleted",
            description = "Total number of records deleted from the table."
        )
        private Long recordCount;

        @Schema(
            title = "Commit instant timestamp",
            description = "The instant timestamp generated for this commit on the Hudi active timeline."
        )
        private String commitInstantTime;

        @Schema(
            title = "Affected partition paths",
            description = "List of partition paths modified during the delete operation."
        )
        private List<String> affectedPartitions;
    }
}
