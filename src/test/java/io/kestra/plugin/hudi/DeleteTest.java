package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
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
    void deletesFromPartitionedTable() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);

        Insert.builder()
            .id(IdUtils.create())
            .type(Insert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_part"))
            .recordKeyField(Property.ofValue("order_id"))
            .partitionPathField(Property.ofValue("region"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", 1, "region", "eu", "amount", 10.5),
                Map.of("order_id", 2, "region", "us", "amount", 20.0)
            )))
            .build()
            .run(runContext);

        Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_part"))
            .recordKeyField(Property.ofValue("order_id"))
            .partitionPathField(Property.ofValue("region"))
            .precombineField(Property.ofValue("amount"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", 2, "region", "us", "amount", 25.0),
                Map.of("order_id", 3, "region", "eu", "amount", 7.0)
            )))
            .build()
            .run(runContext);

        var output = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_part"))
            .recordKeyField(Property.ofValue("order_id"))
            .partitionPathField(Property.ofValue("region"))
            .from(Property.ofValue("[{\"order_id\":1,\"region\":\"eu\"}]"))
            .build()
            .run(runContext);

        assertThat(output.getRecordCount()).isEqualTo(1L);
        assertThat(output.getAffectedPartitions()).containsExactly("eu");
    }

    @Test
    void bareKeysOnPartitionedTableFailInsteadOfDeletingNothing() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);

        Insert.builder()
            .id(IdUtils.create())
            .type(Insert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_bare"))
            .recordKeyField(Property.ofValue("order_id"))
            .partitionPathField(Property.ofValue("region"))
            .from(Property.ofValue(List.of(Map.of("order_id", "u3", "region", "us", "amount", 1.0))))
            .build()
            .run(runContext);

        var task = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_bare"))
            .recordKeyField(Property.ofValue("order_id"))
            .partitionPathField(Property.ofValue("region"))
            .from(Property.ofValue(List.of("u3")))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("'from'")
            .hasMessageContaining("partition path")
            .hasMessageContaining("partitionPathField");
    }

    @Test
    void runDelete() throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = HudiTestUtils.tableUri(runContext);

        // Step 1: Insert records
        Upsert insertTask = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_delete"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "301", "updated_at", 1000L, "status", "OPEN"),
                Map.of("order_id", "302", "updated_at", 1000L, "status", "OPEN")
            )))
            .build();
        insertTask.run(runContext);

        // Step 2: Delete record 301
        Delete deleteTask = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_delete"))
            .recordKeyField(Property.ofValue("order_id"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "301")
            )))
            .build();

        Delete.Output deleteOutput = deleteTask.run(runContext);

        assertThat(deleteOutput).isNotNull();
        assertThat(deleteOutput.getRecordCount()).isEqualTo(1L);
        assertThat(deleteOutput.getCommitInstantTime()).isNotBlank();

        var rows = HudiTestUtils.readLatestRecords(basePath);
        assertThat(rows).hasSize(1);
        assertThat(HudiTestUtils.field(rows.getFirst(), "order_id")).isEqualTo("302");
    }

    @Test
    void failsOnMissingTable() {
        var runContext = runContextFactory.of();
        var task = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .basePath(Property.ofValue(HudiTestUtils.tableUri(runContext)))
            .tableName(Property.ofValue("orders_delete"))
            .recordKeyField(Property.ofValue("order_id"))
            .from(Property.ofValue(List.of(Map.of("order_id", "301"))))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No Hudi table at");
        assertThat(HudiTestUtils.tablePath(runContext).resolve(".hoodie")).doesNotExist();
    }

    @Test
    void invalidJsonInFromNamesTheProperty() {
        var runContext = runContextFactory.of();

        assertThatThrownBy(() -> HudiRecordUtils.parseDeleteKeys(runContext, "[{\"order_id\": ", "order_id", null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("'from'")
            .hasMessageContaining("not valid JSON");
    }
}
