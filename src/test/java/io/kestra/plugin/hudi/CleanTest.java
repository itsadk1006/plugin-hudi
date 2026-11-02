package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class CleanTest {

    @Inject
    private RunContextFactory runContextFactory;

    private void upsert(String basePath, long updatedAt, String status) throws Exception {
        Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_clean"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(Map.of("order_id", "1", "updated_at", updatedAt, "status", status))))
            .build()
            .run(runContextFactory.of());
    }

    @Test
    void runClean(@TempDir Path tempDir) throws Exception {
        var basePath = tempDir.toUri().toString();
        upsert(basePath, 1000L, "CREATED");
        upsert(basePath, 2000L, "PENDING");
        upsert(basePath, 3000L, "COMPLETED");

        var task = Clean.builder()
            .id(IdUtils.create())
            .type(Clean.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_clean"))
            .configuration(Property.ofValue(Map.of("hoodie.cleaner.commits.retained", "1")))
            .build();

        var runOutput = task.run(runContextFactory.of());

        assertThat(runOutput.getInstantTime()).isNotBlank();
        assertThat(runOutput.getFilesCleaned()).isGreaterThanOrEqualTo(1L);

        var rows = HudiTestUtils.readLatestRecords(basePath);
        assertThat(rows).hasSize(1);
        assertThat(HudiTestUtils.field(rows.getFirst(), "status")).isEqualTo("COMPLETED");
    }

    @Test
    void failsOnMissingTable(@TempDir Path tempDir) {
        var task = Clean.builder()
            .id(IdUtils.create())
            .type(Clean.class.getName())
            .basePath(Property.ofValue(tempDir.toUri().toString()))
            .tableName(Property.ofValue("orders_clean"))
            .build();
        var runContext = runContextFactory.of();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No Hudi table at");
    }
}
