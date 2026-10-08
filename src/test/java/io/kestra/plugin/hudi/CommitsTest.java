package io.kestra.plugin.hudi;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import jakarta.inject.Inject;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class CommitsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runFetch(@TempDir Path tempDir) throws Exception {
        var runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .fetchType(Property.of(FetchType.FETCH))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        // Since it's a mock implementation, size is 0L
        assertThat(runOutput.getSize()).isEqualTo(0L);
    }

    @Test
    void runFetchOne(@TempDir Path tempDir) throws Exception {
        var runContext = runContextFactory.of();
        String basePath = tempDir.toUri().toString();

        Commits task = Commits.builder()
            .id(IdUtils.create())
            .type(Commits.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of("orders"))
            .fetchType(Property.of(FetchType.FETCH_ONE))
            .build();

        Commits.Output runOutput = task.run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getSize()).isEqualTo(0L);
    }
}
