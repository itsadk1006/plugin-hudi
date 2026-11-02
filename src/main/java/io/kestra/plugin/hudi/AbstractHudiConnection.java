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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
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
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
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
                    Field field = NativeIO.class.getDeclaredField("nativeLoaded");
                    field.setAccessible(true);
                    field.set(null, false);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    @Schema(
        title = "The base path of the Hudi table",
        description = "Path where the Hudi table is located (e.g., `orders`, `s3a://bucket/table`). Local paths (relative, absolute or `file://`) must be inside the working directory, and relative paths are resolved against it. Remote schemes are used as is."
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
        description = "Key-value configuration map (e.g., Hadoop/AWS/GCS credentials, Hudi write configs). Implementation class keys are not allowed: keys ending with `.impl`, `.class`, `.classname`, `.classnames` or `.factory`, or containing a `.impl.`, `.class.` or `.factory.` segment."
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<Map<String, String>> configuration;

    public Configuration buildHadoopConfiguration(RunContext runContext) throws IllegalVariableEvaluationException {
        return hadoopConfiguration(runContext, this.configuration);
    }

    public static Configuration hadoopConfiguration(RunContext runContext, Property<Map<String, String>> configuration) throws IllegalVariableEvaluationException {
        disableNativeIOOnWindows();

        var hadoopConf = new Configuration();
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            if (System.getProperty("hadoop.home.dir") == null && System.getenv("HADOOP_HOME") == null) {
                var hadoopHome = new File("C:\\hadoop");
                if (hadoopHome.exists()) {
                    System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath());
                }
            }
        }

        renderConfiguration(runContext, configuration).forEach(hadoopConf::set);
        return hadoopConf;
    }

    private static Map<String, String> renderConfiguration(RunContext runContext, Property<Map<String, String>> configuration) throws IllegalVariableEvaluationException {
        var result = new LinkedHashMap<String, String>();
        if (configuration == null) {
            return result;
        }

        var rendered = runContext.render(configuration).asMap(String.class, String.class);
        if (rendered != null) {
            rendered.forEach((key, val) -> {
                validateConfigurationKey(key);
                if (val != null) {
                    result.put(key, val);
                }
            });
        }
        return result;
    }

    /**
     * Suffixes and segments (compared in lower case) that designate implementation classes or factories.
     * Credentials, endpoints and plain tuning keys such as {@code hoodie.cleaner.commits.retained} are not affected.
     */
    private static final List<String> DENIED_KEY_SUFFIXES = List.of(".impl", ".class", ".classname", ".classnames", ".factory", ".provider.class");
    private static final List<String> DENIED_KEY_SEGMENTS = List.of(".impl.", ".class.", ".factory.");

    static void validateConfigurationKey(String key) {
        var lower = key == null ? "" : key.toLowerCase(Locale.ROOT);
        if (DENIED_KEY_SUFFIXES.stream().anyMatch(lower::endsWith) || DENIED_KEY_SEGMENTS.stream().anyMatch(lower::contains)) {
            throw new IllegalArgumentException("Configuration key '" + key + "' is not allowed: implementation class keys cannot be set, remove it from 'configuration'");
        }
    }

    private static final Pattern SCHEME = Pattern.compile("^([a-zA-Z][a-zA-Z0-9+.-]+):");

    /**
     * Renders {@code basePath}. Remote schemes are returned unchanged; local paths (scheme-less or {@code file:})
     * are resolved against the working directory and must stay inside it once normalized and symlinks are resolved.
     */
    public static String resolveBasePath(RunContext runContext, Property<String> basePath) throws IllegalVariableEvaluationException, IOException {
        var rendered = runContext.render(basePath).as(String.class)
            .filter(v -> !v.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("basePath is required"));

        var schemeMatcher = SCHEME.matcher(rendered);
        String localPath;
        if (schemeMatcher.find()) {
            if (!"file".equalsIgnoreCase(schemeMatcher.group(1))) {
                return rendered;
            }
            try {
                var uri = new URI(rendered);
                if (uri.getPath() == null || (uri.getAuthority() != null && !uri.getAuthority().isEmpty())) {
                    throw new IllegalArgumentException("Invalid basePath '" + rendered + "', use a path under the working directory, e.g. a relative path or file:///<working directory>/table");
                }
                localPath = uri.getPath();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid basePath '" + rendered + "': " + e.getMessage(), e);
            }
        } else {
            localPath = rendered;
        }

        var workingDir = runContext.workingDir().path().toRealPath();
        var real = realPathOfExistingPrefix(workingDir.resolve(localPath).normalize());
        if (!real.startsWith(workingDir)) {
            throw new IllegalArgumentException("basePath '" + rendered + "' must be inside the working directory, use a path under the working directory (relative paths are resolved against it)");
        }

        var uri = real.toUri().toString();
        return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    }

    private static java.nio.file.Path realPathOfExistingPrefix(java.nio.file.Path path) throws IOException {
        var existing = path;
        while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            existing = existing.getParent();
        }
        if (existing == null) {
            return path;
        }
        return existing.toRealPath().resolve(existing.relativize(path)).normalize();
    }

    public static boolean tableExists(Configuration hadoopConf, String rBasePath) throws IOException {
        var tablePath = new Path(rBasePath);
        var fs = tablePath.getFileSystem(hadoopConf);
        return fs.exists(new Path(tablePath, HoodieTableMetaClient.METAFOLDER_NAME));
    }

    public HoodieTableMetaClient getExistingMetaClient(RunContext runContext, String rBasePath) throws Exception {
        var hadoopConf = buildHadoopConfiguration(runContext);
        if (!tableExists(hadoopConf, rBasePath)) {
            throw new IllegalArgumentException("No Hudi table at '" + rBasePath + "', check basePath");
        }

        return HoodieTableMetaClient.builder()
            .setConf(new HadoopStorageConfiguration(hadoopConf))
            .setBasePath(rBasePath)
            .build();
    }

    public HoodieTableMetaClient getOrInitMetaClient(
        RunContext runContext,
        String rBasePath,
        String rTableName,
        HoodieTableType tableType
    ) throws Exception {
        var hadoopConf = buildHadoopConfiguration(runContext);
        if (tableExists(hadoopConf, rBasePath)) {
            return getExistingMetaClient(runContext, rBasePath);
        }

        return HoodieTableMetaClient.newTableBuilder()
            .setTableType(tableType != null ? tableType : HoodieTableType.COPY_ON_WRITE)
            .setTableName(rTableName)
            .setBaseFileFormat(HoodieFileFormat.PARQUET.name())
            .initTable(new HadoopStorageConfiguration(hadoopConf), rBasePath);
    }

    public static void commitOrFail(
        HoodieJavaWriteClient<HoodieAvroPayload> client,
        String instantTime,
        List<WriteStatus> statuses,
        String rTableName
    ) {
        for (var status : statuses) {
            if (status.hasErrors()) {
                var firstError = status.getGlobalError() != null
                    ? status.getGlobalError()
                    : status.getErrors().values().stream().findFirst().orElse(null);
                throw new IllegalStateException(
                    "Write to Hudi table '" + rTableName + "' failed with " + status.getTotalErrorRecords()
                        + " error record(s), first error: " + (firstError != null ? firstError.getMessage() : "unknown"),
                    firstError
                );
            }
        }

        if (!client.commit(instantTime, statuses)) {
            throw new IllegalStateException("Commit of instant " + instantTime + " on Hudi table '" + rTableName + "' failed");
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
            Properties customProps = new Properties();
            renderConfiguration(runContext, this.configuration).forEach(customProps::setProperty);
            builder.withProperties(customProps);
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
