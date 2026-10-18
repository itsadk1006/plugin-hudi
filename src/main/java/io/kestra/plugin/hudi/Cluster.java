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
            title = "Cluster an Apache Hudi table",
            code = """
                id: hudi_cluster_flow
                namespace: company.team

                tasks:
                  - id: cluster_table
                    type: io.kestra.plugin.hudi.Cluster
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                """
        )
    }
)
@Schema(
    title = "Cluster an Apache Hudi table",
    description = "Trigger clustering table service on an Apache Hudi table."
)
public class Cluster extends AbstractHudiConnection implements RunnableTask<Cluster.Output> {

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

        logger.info("Clustering Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        getOrInitMetaClient(runContext, renderedBasePath, renderedTableName, HoodieTableType.COPY_ON_WRITE);

        HoodieWriteConfig writeConfig = baseWriteConfigBuilder(runContext, renderedBasePath, renderedTableName).build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createJavaWriteClient(runContext, writeConfig)) {
            activeClient.set(client);
            if (isKilled.get()) {
                throw new IllegalStateException("Cluster task was killed before execution");
            }

            Option<String> clusteringInstant = client.scheduleClustering(Option.empty());
            if (clusteringInstant.isPresent()) {
                String instant = clusteringInstant.get();
                runningInstant.set(instant);

                if (isKilled.get()) {
                    client.rollback(instant);
                    throw new IllegalStateException("Cluster task was killed during execution");
                }

                client.cluster(instant, true);
                logger.info("Successfully completed clustering for instant {} on table '{}'", instant, renderedTableName);
                return Output.builder().instantTime(instant).build();
            } else {
                logger.info("No clustering scheduled for table '{}'", renderedTableName);
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
            title = "Clustering instant timestamp",
            description = "The timestamp of the scheduled and executed clustering instant."
        )
        private String instantTime;
    }
}
