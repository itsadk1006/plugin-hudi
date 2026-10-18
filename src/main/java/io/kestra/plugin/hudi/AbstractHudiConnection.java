package io.kestra.plugin.hudi;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.File;
import java.util.Map;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHudiConnection extends Task {

    static {
        disableNativeIOOnWindows();
    }

    public static void disableNativeIOOnWindows() {
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            String[] possibleDllPaths = new String[]{
                System.getenv("HADOOP_HOME") != null ? System.getenv("HADOOP_HOME") + File.separator + "bin" + File.separator + "hadoop.dll" : null,
                System.getProperty("hadoop.home.dir") != null ? System.getProperty("hadoop.home.dir") + File.separator + "bin" + File.separator + "hadoop.dll" : null,
                "C:\\hadoop\\bin\\hadoop.dll"
            };
            boolean loaded = false;
            for (String p : possibleDllPaths) {
                if (p != null) {
                    File dll = new File(p);
                    if (dll.exists()) {
                        try {
                            System.load(dll.getAbsolutePath());
                            loaded = true;
                            break;
                        } catch (Throwable ignored) {
                        }
                    }
                }
            }
            if (!loaded) {
                try {
                    Class.forName("org.apache.hadoop.io.nativeio.NativeIO");
                    java.lang.reflect.Field field = org.apache.hadoop.io.nativeio.NativeIO.class.getDeclaredField("nativeLoaded");
                    field.setAccessible(true);
                    field.set(null, false);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Schema(
        title = "The base path of the Hudi table",
        description = "Path where the Hudi table is located (e.g., file:///path/to/table, s3a://bucket/table)."
    )
    @PluginProperty(group = "main")
    @NotNull
    protected Property<String> basePath;

    @Schema(
        title = "The name of the Hudi table",
        description = "Target Apache Hudi table name."
    )
    @PluginProperty(group = "main")
    @NotNull
    protected Property<String> tableName;

    @Schema(
        title = "Configuration properties for the Hudi connection",
        description = "Key-value configuration map (e.g., Hadoop/AWS/GCS credentials, Hudi write configs)."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<Map<String, String>> configuration;

    public Configuration buildHadoopConfiguration(RunContext runContext) throws IllegalVariableEvaluationException {
        disableNativeIOOnWindows();

        Configuration hadoopConf = new Configuration();
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            if (System.getProperty("hadoop.home.dir") == null && System.getenv("HADOOP_HOME") == null) {
                File hadoopHome = new File("C:\\hadoop");
                if (hadoopHome.exists()) {
                    System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath());
                }
            }
        }

        if (this.configuration != null) {
            Map<String, String> rendered = runContext.render(this.configuration).asMap(String.class, String.class);
            if (rendered != null) {
                rendered.forEach((key, val) -> {
                    if (val != null) {
                        hadoopConf.set(key, val);
                    }
                });
            }
        }
        return hadoopConf;
    }

    public HoodieTableMetaClient getOrInitMetaClient(
        RunContext runContext,
        String renderedBasePath,
        String renderedTableName,
        HoodieTableType tableType
    ) throws Exception {
        Configuration hadoopConf = buildHadoopConfiguration(runContext);
        HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(hadoopConf);
        Path tablePath = new Path(renderedBasePath);
        FileSystem fs = tablePath.getFileSystem(hadoopConf);
        Path hoodieDir = new Path(tablePath, HoodieTableMetaClient.METAFOLDER_NAME);

        if (!fs.exists(hoodieDir)) {
            return HoodieTableMetaClient.newTableBuilder()
                .setTableType(tableType != null ? tableType : HoodieTableType.COPY_ON_WRITE)
                .setTableName(renderedTableName)
                .setBaseFileFormat(HoodieFileFormat.PARQUET.name())
                .initTable(storageConf, renderedBasePath);
        } else {
            return HoodieTableMetaClient.builder()
                .setConf(storageConf)
                .setBasePath(renderedBasePath)
                .build();
        }
    }

    public HoodieWriteConfig.Builder baseWriteConfigBuilder(
        RunContext runContext,
        String renderedBasePath,
        String renderedTableName
    ) throws IllegalVariableEvaluationException {
        HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
            .withPath(renderedBasePath)
            .forTable(renderedTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
            .withEmbeddedTimelineServerEnabled(false);

        // Apply custom configs
        if (this.configuration != null) {
            Map<String, String> rendered = runContext.render(this.configuration).asMap(String.class, String.class);
            if (rendered != null) {
                Properties customProps = new Properties();
                rendered.forEach((k, v) -> {
                    if (v != null) {
                        customProps.setProperty(k, v);
                    }
                });
                builder.withProperties(customProps);
            }
        }

        return builder;
    }

    public HoodieJavaWriteClient<HoodieAvroPayload> createJavaWriteClient(
        RunContext runContext,
        HoodieWriteConfig writeConfig
    ) throws IllegalVariableEvaluationException {
        Configuration hadoopConf = buildHadoopConfiguration(runContext);
        HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf));
        return new HoodieJavaWriteClient<>(engineContext, writeConfig);
    }
}
