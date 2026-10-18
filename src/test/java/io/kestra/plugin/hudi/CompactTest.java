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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CompactTest {

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
    void runCompact(@TempDir Path tempDir) throws Exception {
        RunContext runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        Compact task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders_mor"))
            .build();

        Compact.Output runOutput = task.run(runContext);
        assertThat(runOutput).isNotNull();
    }

    @Test
    void testKillCancellation(@TempDir Path tempDir) {
        String basePath = tempDir.toUri().toString();

        Compact task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders_mor"))
            .build();

        task.kill();

        RunContext runContext = runContextFactory.of();
        assertThrows(IllegalStateException.class, () -> task.run(runContext));
    }
}
