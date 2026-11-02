package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class ClusterTest {

    @Inject
    private RunContextFactory runContextFactory;

    private Cluster cluster(String basePath) {
        return Cluster.builder()
            .id(IdUtils.create())
            .type(Cluster.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_cluster"))
            .build();
    }

    @Test
    void runCluster() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        for (var i = 1; i <= 3; i++) {
            HudiTestUtils.upsert(runContext, basePath, "orders_cluster", List.of(Map.of("id", "" + i, "ts", 1000L, "val", "v" + i)));
        }
        var fileGroupsBefore = HudiTestUtils.latestBaseFileInstants(basePath);
        assertThat(fileGroupsBefore).hasSize(3);

        var runOutput = cluster(basePath).run(runContext);

        assertThat(runOutput.getInstantTime()).isNotBlank();
        var timeline = HudiTestUtils.metaClient(basePath).getActiveTimeline();
        assertThat(timeline.getCompletedReplaceTimeline().getInstantsAsStream().map(i -> i.requestedTime())).contains(runOutput.getInstantTime());
        assertThat(HudiTestUtils.latestBaseFileInstants(basePath)).containsExactly(runOutput.getInstantTime());
        assertThat(HudiTestUtils.readLatestRecords(basePath))
            .extracting(r -> HudiTestUtils.field(r, "id") + "=" + HudiTestUtils.field(r, "val"))
            .containsExactlyInAnyOrder("1=v1", "2=v2", "3=v3");
    }

    @Test
    void killRollsBackAScheduledClusteringPlan() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        for (var i = 1; i <= 2; i++) {
            HudiTestUtils.upsert(runContext, basePath, "orders_cluster", List.of(Map.of("id", "" + i, "ts", 1000L, "val", "v" + i)));
        }

        var task = cluster(basePath);
        var writeConfig = task.baseWriteConfigBuilder(runContext, basePath, "orders_cluster")
            .withSchema(new TableSchemaResolver(HudiTestUtils.metaClient(basePath)).getTableSchemaIfPresent(false).get().toString())
            .withClusteringConfig(HoodieClusteringConfig.newBuilder()
                .withClusteringPlanStrategyClass(JavaSizeBasedClusteringPlanStrategy.class.getName())
                .withClusteringExecutionStrategyClass(JavaSortAndSizeExecutionStrategy.class.getName())
                .build())
            .build();
        try (var client = task.createJavaWriteClient(runContext, writeConfig)) {
            var instant = client.scheduleClustering(Option.empty()).get();
            assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().filterPendingReplaceOrClusteringTimeline().countInstants()).isEqualTo(1);

            task.kill();

            assertThatThrownBy(() -> task.rollbackIfKilled(client, instant)).isInstanceOf(IllegalStateException.class);
        }
        assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().filterPendingReplaceOrClusteringTimeline().countInstants()).isZero();
        assertThat(HudiTestUtils.readLatestRecords(basePath)).hasSize(2);
    }

    @Test
    void failsOnMissingTable() {
        var runContext = runContextFactory.of();
        var task = cluster(HudiTestUtils.tableUri(runContext));

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No Hudi table at");
    }
}
