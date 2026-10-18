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
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                """
        )
    }
)
@Schema(
    title = "Compact an Apache Hudi table",
    description = "Trigger compaction table service on an Apache Hudi table."
)
public class Compact extends AbstractHudiConnection implements RunnableTask<Compact.Output> {

    @Builder.Default
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Builder.Default
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<String> runningInstant = new AtomicReference<>();

    @Builder.Default
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<HoodieJavaWriteClient<HoodieAvroPayload>> activeClient = new AtomicReference<>();

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedBasePath = runContext.render(this.basePath).as(String.class).orElseThrow();
        String renderedTableName = runContext.render(this.tableName).as(String.class).orElseThrow();

        logger.info("Compacting Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        getOrInitMetaClient(runContext, renderedBasePath, renderedTableName, HoodieTableType.MERGE_ON_READ);

        HoodieWriteConfig writeConfig = baseWriteConfigBuilder(runContext, renderedBasePath, renderedTableName).build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createJavaWriteClient(runContext, writeConfig)) {
            activeClient.set(client);
            if (isKilled.get()) {
                throw new IllegalStateException("Compact task was killed before execution");
            }

            Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
            if (compactionInstant.isPresent()) {
                String instant = compactionInstant.get();
                runningInstant.set(instant);

                if (isKilled.get()) {
                    client.rollback(instant);
                    throw new IllegalStateException("Compact task was killed during execution");
                }

                client.compact(instant);
                logger.info("Successfully completed compaction for instant {} on table '{}'", instant, renderedTableName);
                return Output.builder().instantTime(instant).build();
            } else {
                logger.info("No compaction scheduled for table '{}'", renderedTableName);
                return Output.builder().instantTime(null).build();
            }
        } finally {
            activeClient.set(null);
            runningInstant.set(null);
        }
    }

    @Override
    public void kill() {
        isKilled.set(true);
        String instant = runningInstant.get();
        HoodieJavaWriteClient<HoodieAvroPayload> client = activeClient.get();
        if (client != null && instant != null) {
            try {
                client.rollback(instant);
            } catch (Exception ignored) {
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Compaction instant timestamp",
            description = "The timestamp of the scheduled and executed compaction instant."
        )
        private String instantTime;
    }
}
