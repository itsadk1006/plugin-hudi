package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class BasePathAndConfigurationTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static String resolve(RunContext runContext, String basePath) throws Exception {
        return AbstractHudiConnection.resolveBasePath(runContext, Property.ofValue(basePath));
    }

    @Test
    void relativePathIsResolvedInWorkingDir() throws Exception {
        var runContext = runContextFactory.of();
        var workingDir = runContext.workingDir().path().toRealPath();

        var resolved = resolve(runContext, "tables/orders");

        assertThat(Path.of(java.net.URI.create(resolved))).isEqualTo(workingDir.resolve("tables/orders"));
    }

    @Test
    void absolutePathAndFileUriInsideWorkingDirAreAccepted() throws Exception {
        var runContext = runContextFactory.of();
        var inside = runContext.workingDir().path().resolve("orders");

        assertThatCode(() -> resolve(runContext, inside.toString())).doesNotThrowAnyException();
        assertThatCode(() -> resolve(runContext, inside.toUri().toString())).doesNotThrowAnyException();
    }

    @Test
    void traversalIsRejected() {
        var runContext = runContextFactory.of();

        assertThatThrownBy(() -> resolve(runContext, "../outside"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
        assertThatThrownBy(() -> resolve(runContext, "tables/../../outside"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
    }

    @Test
    void absolutePathOutsideWorkingDirIsRejected(@TempDir Path outside) {
        var runContext = runContextFactory.of();

        assertThatThrownBy(() -> resolve(runContext, outside.toString()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
        assertThatThrownBy(() -> resolve(runContext, outside.toUri().toString()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
    }

    @Test
    void symlinkToOutsideIsRejected(@TempDir Path outside) throws Exception {
        var runContext = runContextFactory.of();
        Files.createSymbolicLink(runContext.workingDir().path().resolve("link"), outside);

        assertThatThrownBy(() -> resolve(runContext, "link"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
        assertThatThrownBy(() -> resolve(runContext, "link/new_table"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
    }

    @Test
    void remoteSchemeIsPassedThrough() throws Exception {
        var runContext = runContextFactory.of();
        for (var basePath : List.of("s3a://bucket/table", "gs://bucket/table", "abfs://container@account.dfs.core.windows.net/table", "hdfs://namenode:8020/table")) {
            assertThat(resolve(runContext, basePath)).isEqualTo(basePath);
        }
    }

    @Test
    void taskRejectsPathOutsideWorkingDir(@TempDir Path outside) {
        var runContext = runContextFactory.of();
        var task = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(outside.toString()))
            .tableName(Property.ofValue("orders"))
            .recordKeyField(Property.ofValue("id"))
            .from(Property.ofValue(List.of(Map.of("id", "1"))))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be inside the working directory");
        assertThat(outside.resolve(".hoodie")).doesNotExist();
    }

    @Test
    void deniedConfigurationKeysAreRejected() {
        var runContext = runContextFactory.of();
        var keys = List.of(
            "fs.s3a.impl", "fs.custom.impl", "FS.S3A.IMPL", "fs.impl.disable.cache.extra", "fs.s3a.aws.credentials.provider.class",
            "hoodie.datasource.write.payload.class", "hoodie.some.classname", "hoodie.some.classnames", "hoodie.write.some.factory",
            "hoodie.index.class.name", "hoodie.Some.Class"
        );

        for (var key : keys) {
            assertThatThrownBy(() -> AbstractHudiConnection.hadoopConfiguration(runContext, Property.ofValue(Map.of(key, "value"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'" + key + "'")
                .hasMessageContaining("not allowed");
        }
    }

    @Test
    void deniedConfigurationKeyIsRejectedByTask() {
        var runContext = runContextFactory.of();
        var task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.ofValue("table"))
            .tableName(Property.ofValue("orders"))
            .configuration(Property.ofValue(Map.of("hoodie.compact.some.class", "x")))
            .build();

        assertThatThrownBy(() -> task.baseWriteConfigBuilder(runContext, "file:///table", "orders"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("'hoodie.compact.some.class' is not allowed");
    }

    @Test
    void allowedConfigurationKeysPass() throws Exception {
        var runContext = runContextFactory.of();
        var configuration = Map.of(
            "fs.s3a.access.key", "key",
            "fs.s3a.secret.key", "secret",
            "fs.s3a.endpoint", "http://localhost:9000",
            "fs.s3a.path.style.access", "true",
            "hoodie.cleaner.commits.retained", "1"
        );

        var hadoopConf = AbstractHudiConnection.hadoopConfiguration(runContext, Property.ofValue(configuration));

        configuration.forEach((k, v) -> assertThat(hadoopConf.get(k)).isEqualTo(v));
    }
}
