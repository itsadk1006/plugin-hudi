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
class DeleteTest {

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
    void runDelete(@TempDir Path tempDir) throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        // Step 1: Insert records
        Upsert insertTask = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders_delete"))
            .recordKeyField(Property.of("order_id"))
            .precombineField(Property.of("updated_at"))
            .from(Property.of(List.of(
                Map.of("order_id", "301", "updated_at", 1000L, "status", "OPEN"),
                Map.of("order_id", "302", "updated_at", 1000L, "status", "OPEN")
            )))
            .build();
        insertTask.run(runContext);

        // Step 2: Delete record 301
        Delete deleteTask = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders_delete"))
            .recordKeyField(Property.of("order_id"))
            .from(Property.of(List.of(
                Map.of("order_id", "301")
            )))
            .build();

        Delete.Output deleteOutput = deleteTask.run(runContext);

        assertThat(deleteOutput).isNotNull();
        assertThat(deleteOutput.getRecordCount()).isEqualTo(1L);
        assertThat(deleteOutput.getCommitInstantTime()).isNotBlank();
    }
}
