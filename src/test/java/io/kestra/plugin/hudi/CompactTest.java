package io.kestra.plugin.hudi;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import jakarta.inject.Inject;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class CompactTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run(@TempDir Path tempDir) throws Exception {
        var runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        Compact task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .build();

        VoidOutput runOutput = task.run(runContext);

        // Since it's a mock implementation, it returns null
        assertThat(runOutput).isNull();
    }
}
