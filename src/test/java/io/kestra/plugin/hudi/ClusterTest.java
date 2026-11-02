package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
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
        Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_cluster"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "1", "updated_at", 1000L, "status", "CREATED"),
                Map.of("order_id", "2", "updated_at", 1000L, "status", "PENDING")
            )))
            .build()
            .run(runContext);

        var runOutput = cluster(basePath).run(runContext);

        assertThat(runOutput).isNotNull();
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
