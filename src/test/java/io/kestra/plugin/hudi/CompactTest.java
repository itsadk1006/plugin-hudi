package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class CompactTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static Compact compact(String basePath) {
        return Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_mor"))
            .build();
    }

    private static void initMorTable(String basePath) throws Exception {
        HoodieTableMetaClient.newTableBuilder()
            .setTableType(HoodieTableType.MERGE_ON_READ)
            .setTableName("orders_mor")
            .setBaseFileFormat(HoodieFileFormat.PARQUET.name())
            .initTable(new HadoopStorageConfiguration(new Configuration()), basePath);
    }

    @Test
    void runCompact(@TempDir Path tempDir) throws Exception {
        var basePath = tempDir.toUri().toString();
        initMorTable(basePath);

        var runOutput = compact(basePath).run(runContextFactory.of());

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getInstantTime()).isNull();
    }

    @Test
    void failsOnMissingTable(@TempDir Path tempDir) {
        var task = compact(tempDir.toUri().toString());
        var runContext = runContextFactory.of();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No Hudi table at");
        assertThat(tempDir.resolve(".hoodie")).doesNotExist();
    }

    @Test
    void testKillCancellation(@TempDir Path tempDir) throws Exception {
        var basePath = tempDir.toUri().toString();
        initMorTable(basePath);

        var task = compact(basePath);
        task.kill();

        var runContext = runContextFactory.of();
        assertThatThrownBy(() -> task.run(runContext)).isInstanceOf(IllegalStateException.class);
    }
}
