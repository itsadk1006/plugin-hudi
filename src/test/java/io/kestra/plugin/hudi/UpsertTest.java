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
    void tablesInTheSameJvmDoNotShareIndexState() throws Exception {
        for (var round = 0; round < 2; round++) {
            var runContext = runContextFactory.of();
            var basePath = HudiTestUtils.tableUri(runContext);

            Insert.builder()
                .id(IdUtils.create())
                .type(Insert.class.getName())
                .basePath(Property.ofValue(basePath))
                .tableName(Property.ofValue("orders"))
                .recordKeyField(Property.ofValue("order_id"))
                .partitionPathField(Property.ofValue("region"))
                .from(Property.ofValue(List.of(
                    Map.of("order_id", 1, "region", "eu", "amount", 10.5),
                    Map.of("order_id", 2, "region", "us", "amount", 20.0)
                )))
                .build()
                .run(runContext);

            var output = Upsert.builder()
                .id(IdUtils.create())
                .type(Upsert.class.getName())
                .basePath(Property.ofValue(basePath))
                .tableName(Property.ofValue("orders"))
                .recordKeyField(Property.ofValue("order_id"))
                .partitionPathField(Property.ofValue("region"))
                .precombineField(Property.ofValue("amount"))
                .from(Property.ofValue(List.of(
                    Map.of("order_id", 2, "region", "us", "amount", 25.0),
                    Map.of("order_id", 3, "region", "eu", "amount", 7.0)
                )))
                .build()
                .run(runContext);

            assertThat(output.getRecordCount()).isEqualTo(2L);

            var rows = HudiTestUtils.readLatestRecords(basePath, "eu", "us");
            assertThat(rows).extracting(r -> HudiTestUtils.field(r, "order_id")).containsExactlyInAnyOrder("1", "2", "3");
            assertThat(rows)
                .filteredOn(r -> HudiTestUtils.field(r, "order_id").equals("2"))
                .singleElement()
                .satisfies(r -> assertThat(HudiTestUtils.field(r, "amount")).isEqualTo("25.0"));
            assertThat(rows)
                .filteredOn(r -> HudiTestUtils.field(r, "order_id").equals("1"))
                .singleElement()
                .satisfies(r -> assertThat(HudiTestUtils.field(r, "amount")).isEqualTo("10.5"));
        }
    }

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = HudiTestUtils.tableUri(runContext);

        Upsert task = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "1", "updated_at", 1000L, "status", "CREATED"),
                Map.of("order_id", "2", "updated_at", 1000L, "status", "PENDING")
            )))
            .build();

        Upsert.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getRecordCount()).isEqualTo(2L);
        assertThat(runOutput.getCommitInstantTime()).isNotBlank();
        assertThat(HudiTestUtils.readLatestRecords(basePath)).hasSize(2);

        // Update record 1
        Upsert updateTask = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "1", "updated_at", 2000L, "status", "COMPLETED")
            )))
            .build();

        Upsert.Output updateOutput = updateTask.run(runContext);
        assertThat(updateOutput).isNotNull();
        assertThat(updateOutput.getRecordCount()).isEqualTo(1L);
        assertThat(updateOutput.getCommitInstantTime()).isNotBlank();
        assertThat(updateOutput.getCommitInstantTime()).isNotEqualTo(runOutput.getCommitInstantTime());

        var rows = HudiTestUtils.readLatestRecords(basePath);
        assertThat(rows).hasSize(2);
        assertThat(rows)
            .filteredOn(r -> HudiTestUtils.field(r, "order_id").equals("1"))
            .singleElement()
            .satisfies(r -> assertThat(HudiTestUtils.field(r, "status")).isEqualTo("COMPLETED"));
        assertThat(rows)
            .filteredOn(r -> HudiTestUtils.field(r, "order_id").equals("2"))
            .singleElement()
            .satisfies(r -> assertThat(HudiTestUtils.field(r, "status")).isEqualTo("PENDING"));
    }

    @Test
    void failsOnMissingRecordKey() {
        var runContext = runContextFactory.of();
        var task = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(HudiTestUtils.tableUri(runContext)))
            .tableName(Property.ofValue("orders"))
            .recordKeyField(Property.ofValue("order_id"))
            .precombineField(Property.ofValue("updated_at"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "1", "updated_at", 1000L),
                Map.of("order_idd", "2", "updated_at", 1000L)
            )))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Record at index 1 has no 'order_id' field");
    }
}
