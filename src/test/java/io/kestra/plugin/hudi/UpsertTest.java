package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class UpsertTest {

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void setupHadoop() {
        AbstractHudiConnection.disableNativeIOOnWindows();
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            File hadoopHome = new File("C:\\hadoop");
            if (hadoopHome.exists()) {
                System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath());
            }
        }
    }

    @Test
    void run(@TempDir Path tempDir) throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        Upsert task = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .recordKeyField(Property.of("order_id"))
            .precombineField(Property.of("updated_at"))
            .from(Property.of(List.of(
                Map.of("order_id", "1", "updated_at", 1000L, "status", "CREATED"),
                Map.of("order_id", "2", "updated_at", 1000L, "status", "PENDING")
            )))
            .build();

        Upsert.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getRecordCount()).isEqualTo(2L);
        assertThat(runOutput.getCommitInstantTime()).isNotBlank();

        // Update record 1
        Upsert updateTask = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .recordKeyField(Property.of("order_id"))
            .precombineField(Property.of("updated_at"))
            .from(Property.of(List.of(
                Map.of("order_id", "1", "updated_at", 2000L, "status", "COMPLETED")
            )))
            .build();

        Upsert.Output updateOutput = updateTask.run(runContext);
        assertThat(updateOutput).isNotNull();
        assertThat(updateOutput.getRecordCount()).isEqualTo(1L);
        assertThat(updateOutput.getCommitInstantTime()).isNotBlank();
        assertThat(updateOutput.getCommitInstantTime()).isNotEqualTo(runOutput.getCommitInstantTime());
    }
}
