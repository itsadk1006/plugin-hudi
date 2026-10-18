package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
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
class CommitsTest {

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

    private void createCommit(RunContext runContext, String basePath) throws Exception {
        Upsert task = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .recordKeyField(Property.of("order_id"))
            .precombineField(Property.of("updated_at"))
            .from(Property.of(List.of(
                Map.of("order_id", "101", "updated_at", 1000L, "status", "SUCCESS")
            )))
            .build();
        task.run(runContext);
    }

    @Test
    void runFetch(@TempDir Path tempDir) throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        createCommit(runContext, basePath);

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .fetchType(Property.of(FetchType.FETCH))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isGreaterThanOrEqualTo(1L);
        assertThat(runOutput.getRows()).isNotEmpty();
        Map<String, Object> firstCommit = runOutput.getRows().get(0);
        assertThat(firstCommit).containsKey("instantTime");
        assertThat(firstCommit).containsKey("action");
        assertThat(firstCommit.get("state")).isEqualTo("COMPLETED");
    }

    @Test
    void runFetchOne(@TempDir Path tempDir) throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        createCommit(runContext, basePath);

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .fetchType(Property.of(FetchType.FETCH_ONE))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isEqualTo(1L);
        assertThat(runOutput.getRow()).isNotNull();
        assertThat(runOutput.getRow()).containsKey("instantTime");
    }

    @Test
    void runStore(@TempDir Path tempDir) throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        createCommit(runContext, basePath);

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .fetchType(Property.of(FetchType.STORE))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isGreaterThanOrEqualTo(1L);
        assertThat(runOutput.getUri()).isNotNull();
    }
}
