package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.hudi.common.model.HoodieKey;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

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
                    basePath: hdfs://namenode:8020/data/hudi/orders
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
    description = """
        Delete records from an Apache Hudi table using HoodieJavaWriteClient. \
        On a partitioned table, `from` must contain the partition path of each record, otherwise the task fails instead of deleting nothing.
        """
)
public class Delete extends AbstractHudiConnection implements RunnableTask<Delete.Output> {

    @Schema(
        title = "Record key field",
        description = """
            Field name to extract the record key from incoming records.
            """
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<String> recordKeyField;

    @Schema(
        title = "Partition path field",
        description = """
            Field name to extract the partition path. Defaults to empty for non-partitioned tables.
            """
    )
    @PluginProperty(group = "processing")
    private Property<String> partitionPathField;

    @Schema(
        title = "Source keys or records",
        description = """
            Keys or records to delete. Can be a list of maps, strings, or a Kestra internal storage URI (kestra://...). \
            When `partitionPathField` is set (partitioned table) every entry must be a record containing both the record key field and the partition path field, \
            because a bare key cannot be located; the task fails otherwise. Bare keys only work on non-partitioned tables.
            """
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<Object> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rBasePath = AbstractHudiConnection.resolveBasePath(runContext, this.basePath);
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));
        var rRecordKey = runContext.render(this.recordKeyField).as(String.class).orElseThrow(() -> new IllegalArgumentException("recordKeyField is required"));
        var rPartition = this.partitionPathField != null ? runContext.render(this.partitionPathField).as(String.class).orElse(null) : null;
        var rFrom = runContext.render(this.from).as(Object.class).orElse(null);

        logger.info("Deleting records from Hudi table '{}' at '{}'", rTableName, rBasePath);

        var keys = HudiRecordUtils.parseDeleteKeys(runContext, rFrom, rRecordKey, rPartition);
        if (keys.isEmpty()) {
            logger.warn("No keys to delete from Hudi table '{}'", rTableName);
            return Output.builder()
                .recordCount(0L)
                .commitInstantTime(null)
                .affectedPartitions(Collections.emptyList())
                .build();
        }

        getExistingMetaClient(runContext, rBasePath);
        if (rPartition != null) {
            requirePartitionPaths(keys, rTableName);
        }

        var props = new Properties();
        props.setProperty("hoodie.datasource.write.recordkey.field", rRecordKey);
        if (rPartition != null) {
            props.setProperty("hoodie.datasource.write.partitionpath.field", rPartition);
        }

        var writeConfig = baseWriteConfigBuilder(runContext, rBasePath, rTableName)
            .withProperties(props)
            .build();

        try (var client = createJavaWriteClient(runContext, writeConfig)) {
            var instantTime = client.startCommit();
            var statuses = client.delete(keys, instantTime);
            commitOrFail(client, instantTime, statuses, rTableName);

            var totalDeleted = 0L;
            var affectedPartitions = new LinkedHashSet<String>();
            for (var status : statuses) {
                totalDeleted += status.getStat().getNumDeletes();
                if (status.getPartitionPath() != null && !status.getPartitionPath().isEmpty()) {
                    affectedPartitions.add(status.getPartitionPath());
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

    static void requirePartitionPaths(List<HoodieKey> keys, String tableName) {
        for (var i = 0; i < keys.size(); i++) {
            var partition = keys.get(i).getPartitionPath();
            if (partition == null || partition.isEmpty()) {
                throw new IllegalArgumentException("Entry " + i + " of 'from' has no partition path, but 'partitionPathField' is set for partitioned table '" + tableName + "': "
                    + "a bare key cannot be located. Pass records that contain both the record key and the partition field in 'from', not keys alone");
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of records deleted",
            description = """
                Number of records actually deleted, as reported by the Hudi write statuses. Keys that do not exist are not counted.
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
                List of partition paths modified during the delete operation.
                """
        )
        private List<String> affectedPartitions;
    }
}
