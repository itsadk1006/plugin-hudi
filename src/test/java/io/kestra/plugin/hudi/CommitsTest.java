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

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "101", "updated_at", 1000L, "status", "SUCCESS")
            )))
            .build();
        task.run(runContext);
    }

    @Test
    void runFetch() throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = HudiTestUtils.tableUri(runContext);

        createCommit(runContext, basePath);

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isGreaterThanOrEqualTo(1L);
        assertThat(runOutput.getRows()).isNotEmpty();
        Map<String, Object> firstCommit = runOutput.getRows().getFirst();
        assertThat(firstCommit).containsKey("instantTime");
        assertThat(firstCommit).containsKey("action");
        assertThat(firstCommit.get("state")).isEqualTo("COMPLETED");
        assertThat(firstCommit.get("totalRecords")).isEqualTo(1L);
    }

    @Test
    void runFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = HudiTestUtils.tableUri(runContext);

        createCommit(runContext, basePath);

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isEqualTo(1L);
        assertThat(runOutput.getRow()).isNotNull();
        assertThat(runOutput.getRow()).containsKey("instantTime");
        assertThat(runOutput.getRow().get("totalRecords")).isEqualTo(1L);
        assertThat(runOutput.getRows()).isNull();
    }

    @Test
    void readsClusteringCommits() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        for (var i = 1; i <= 2; i++) {
            HudiTestUtils.upsert(runContext, basePath, "orders", List.of(Map.of("id", "" + i, "ts", 1000L, "val", "v")));
        }
        var clustered = Cluster.builder()
            .id(IdUtils.create())
            .type(Cluster.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .build()
            .run(runContext);
        assertThat(clustered.getInstantTime()).isNotBlank();

        var output = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .build()
            .run(runContext);

        assertThat(output.getRows()).hasSize(3);
        var last = output.getRows().getLast();
        assertThat(last).containsEntry("instantTime", clustered.getInstantTime());
        assertThat(last.get("action")).isEqualTo("replacecommit");
        assertThat(last.get("totalRecords")).isEqualTo(2L);
    }

    @Test
    void failsOnMissingTable() {
        var runContext = runContextFactory.of();
        var task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.ofValue(HudiTestUtils.tableUri(runContext)))
            .tableName(Property.ofValue("orders"))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No Hudi table at");
        assertThat(HudiTestUtils.tablePath(runContext).resolve(".hoodie")).doesNotExist();
    }

    @Test
    void runStore() throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = HudiTestUtils.tableUri(runContext);

        createCommit(runContext, basePath);

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isGreaterThanOrEqualTo(1L);
        assertThat(runOutput.getUri()).isNotNull();
    }
}
