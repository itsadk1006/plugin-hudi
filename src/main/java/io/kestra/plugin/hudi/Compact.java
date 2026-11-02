package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Compact an Apache Hudi table",
            code = """
                id: hudi_compact_flow
                namespace: company.team

                tasks:
                  - id: compact_table
                    type: io.kestra.plugin.hudi.Compact
                    basePath: hdfs://namenode:8020/data/hudi/orders
                    tableName: orders
                """
        )
    }
)
@Schema(
    title = "Compact an Apache Hudi table",
    description = """
        Trigger the compaction table service on an Apache Hudi table. Compaction only applies to merge-on-read tables created outside this plugin, since this plugin creates copy-on-write tables only.
        """
)
public class Compact extends AbstractHudiMaintenance implements RunnableTask<Compact.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rBasePath = AbstractHudiConnection.resolveBasePath(runContext, this.basePath);
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));

        logger.info("Compacting Hudi table '{}' at '{}'", rTableName, rBasePath);

        var metaClient = getExistingMetaClient(runContext, rBasePath);
        if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
            logger.info("Table '{}' is copy on write, there is nothing to compact", rTableName);
            return Output.builder().instantTime(null).build();
        }

        var writeConfig = baseWriteConfigBuilder(runContext, rBasePath, rTableName).build();

        try (var client = createJavaWriteClient(runContext, writeConfig)) {
            ensureNotKilled("before execution");

            // A requested plan is executed first; inflight instants may run elsewhere, so they are never rolled back or executed here.
            var pendingInstants = metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants();
            var requested = pendingInstants.stream().filter(HoodieInstant::isRequested).findFirst();
            if (requested.isEmpty() && !pendingInstants.isEmpty()) {
                throw new IllegalStateException("Compaction instant " + pendingInstants.getFirst().requestedTime() + " on table '" + rTableName
                    + "' is inflight and may be running elsewhere, it is left untouched. Retry once it has completed or been rolled back");
            }
            var compactionInstant = requested.isPresent()
                ? Option.of(requested.get().requestedTime())
                : client.scheduleCompaction(Option.empty());
            if (compactionInstant.isPresent()) {
                var instant = compactionInstant.get();
                ensureNotKilled("after scheduling, the plan " + instant + " stays pending and runs on the next execution");

                client.compact(instant, true);
                logger.info("Successfully completed compaction for instant {} on table '{}'", instant, rTableName);
                return Output.builder().instantTime(instant).build();
            } else {
                logger.info("No compaction scheduled for table '{}'", rTableName);
                return Output.builder().instantTime(null).build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Compaction instant timestamp",
            description = """
                The timestamp of the scheduled and executed compaction instant.
                """
        )
        private String instantTime;
    }
}
