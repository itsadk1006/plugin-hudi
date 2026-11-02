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
        for (var basePath : List.of(
            "s3a://bucket/table", "gs://bucket/table", "abfs://container@account.dfs.core.windows.net/table",
            "abfss://container@account.dfs.core.windows.net/table", "hdfs://namenode:8020/table",
            "wasb://container@account.blob.core.windows.net/table", "wasbs://container@account.blob.core.windows.net/table"
        )) {
            assertThat(resolve(runContext, basePath)).isEqualTo(basePath);
        }
    }

    @Test
    void schemesOutsideTheAllowedListAreRejected() {
        var runContext = runContextFactory.of();
        for (var basePath : List.of(
            "viewfs://cluster/table", "har://hdfs-namenode/table.har", "http://host/table", "https://host/table",
            "ftp://host/table", "s3://bucket/table", "s3n://bucket/table", "unknownscheme://bucket/table"
        )) {
            assertThatThrownBy(() -> resolve(runContext, basePath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed remote schemes")
                .hasMessageContaining("s3a")
                .hasMessageContaining("abfss");
        }
    }

    @Test
    void remoteBasePathRejectsLocalPaths() {
        var runContext = runContextFactory.of();
        for (var basePath : List.of("table", "/tmp/table", "file:///tmp/table", "viewfs://cluster/table")) {
            assertThatThrownBy(() -> AbstractHudiConnection.resolveRemoteBasePath(runContext, Property.ofValue(basePath)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remote table path")
                .hasMessageContaining("working directory");
        }
        assertThatCode(() -> AbstractHudiConnection.resolveRemoteBasePath(runContext, Property.ofValue("s3a://bucket/table")))
            .doesNotThrowAnyException();
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
            "hoodie.write.lock.provider", "hoodie.write.commit.callback.on", "hoodie.write.commit.callback.http.url",
            "hoodie.datasource.write.payload.class", "hoodie.some.classname", "hoodie.write.some.factory", "hoodie.some.impl",
            "hoodie.index.class.name", "HOODIE.Some.Class", "hoodie.some.endpoint", "hoodie.compaction.strategy",
            "io.compression.codecs", "io.serializations", "hadoop.security.group.mapping", "fs.defaultFS",
            "fs.AbstractFileSystem.s3a.impl", "fs.viewfs.mounttable.cluster.link./data", "fs.s3a.impl", "fs.custom.impl", "FS.S3A.IMPL",
            "fs.s3a.aws.credentials.provider", "fs.s3a.some.factory", "fs.gs.some.classname", "fs.azure.account.oauth.provider.type",
            "fs.abfs.some.class", "fs.s3a.custom.signers", "unknown.key"
        );

        for (var key : keys) {
            assertThatThrownBy(() -> AbstractHudiConnection.hadoopConfiguration(runContext, Property.ofValue(Map.of(key, "value"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'" + key + "'")
                .hasMessageContaining("not allowed")
                .hasMessageContaining("documentation");
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
            "fs.s3a.region", "eu-west-1",
            "fs.s3a.session.token", "token",
            "fs.s3a.connection.maximum", "10",
            "fs.gs.project.id", "project",
            "fs.azure.account.key.account.dfs.core.windows.net", "key",
            "hoodie.cleaner.commits.retained", "1"
        );

        var hadoopConf = AbstractHudiConnection.hadoopConfiguration(runContext, Property.ofValue(configuration));

        configuration.forEach((k, v) -> assertThat(hadoopConf.get(k)).isEqualTo(v));
    }
}
