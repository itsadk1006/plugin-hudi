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

@KestraTest
class InsertTest {

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
    void runInsert() throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = HudiTestUtils.tableUri(runContext);

        Insert task = Insert.builder()
            .id(IdUtils.create())
            .type(Insert.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_insert"))
            .recordKeyField(Property.ofValue("order_id"))
            .from(Property.ofValue(List.of(
                Map.of("order_id", "201", "product", "Keyboard", "price", 45.0),
                Map.of("order_id", "202", "product", "Mouse", "price", 25.0)
            )))
            .build();

        Insert.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getRecordCount()).isEqualTo(2L);
        assertThat(runOutput.getCommitInstantTime()).isNotBlank();

        var rows = HudiTestUtils.readLatestRecords(basePath);
        assertThat(rows).hasSize(2);
        assertThat(rows)
            .extracting(r -> HudiTestUtils.field(r, "product"))
            .containsExactlyInAnyOrder("Keyboard", "Mouse");
    }
}
