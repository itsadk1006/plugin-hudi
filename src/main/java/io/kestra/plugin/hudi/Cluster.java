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
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;


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
                    basePath: hudi_tables/orders
                    tableName: orders
                """
        )
    }
)
@Schema(
    title = "Cluster an Apache Hudi table",
    description = "Trigger clustering table service on an Apache Hudi table."
)
public class Cluster extends AbstractHudiMaintenance implements RunnableTask<Cluster.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rBasePath = AbstractHudiConnection.resolveBasePath(runContext, this.basePath);
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));

        logger.info("Clustering Hudi table '{}' at '{}'", rTableName, rBasePath);

        var metaClient = getExistingMetaClient(runContext, rBasePath);

        var writeConfigBuilder = baseWriteConfigBuilder(runContext, rBasePath, rTableName);
        new TableSchemaResolver(metaClient).getTableSchemaIfPresent(false)
            .ifPresent(tableSchema -> writeConfigBuilder.withSchema(tableSchema.toString()));
        var writeConfig = writeConfigBuilder
            .withClusteringConfig(HoodieClusteringConfig.newBuilder()
                .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
                .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
                .build())
            .build();

        try (var client = createJavaWriteClient(runContext, writeConfig)) {
            activeClient.set(client);
            ensureNotKilled("before execution");

            var clusteringInstant = client.scheduleClustering(Option.empty());
            if (clusteringInstant.isPresent()) {
                var instant = clusteringInstant.get();
                runningInstant.set(instant);

                rollbackIfKilled(client, instant);

                client.cluster(instant, true);
                logger.info("Successfully completed clustering for instant {} on table '{}'", instant, rTableName);
                return Output.builder().instantTime(instant).build();
            } else {
                logger.info("No clustering scheduled for table '{}'", rTableName);
                return Output.builder().instantTime(null).build();
            }
        } finally {
            activeClient.set(null);
            runningInstant.set(null);
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
