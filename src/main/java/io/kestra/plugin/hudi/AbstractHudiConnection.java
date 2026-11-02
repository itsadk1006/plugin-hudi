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

    static final String BASE_PATH_DESCRIPTION = "Path where the Hudi table is located (e.g., `orders`, `s3a://bucket/table`). "
        + "Allowed remote schemes: `hdfs`, `s3a`, `gs`, `abfs`, `abfss`, `wasb`, `wasbs`. "
        + "Local paths (relative, absolute or `file://`) must be inside the working directory, and relative paths are resolved against it. "
        + "A local table only lives as long as the working directory of the task.";

    static final String CONFIGURATION_DESCRIPTION = "Key-value configuration map (e.g., Hadoop/AWS/GCS credentials, Hudi write configs). "
        + "Allowed configuration keys start with `hoodie.`, `fs.s3a.`, `fs.gs.`, `fs.azure.` or `fs.abfs.`. "
        + "Within `hoodie.`, keys containing `callback`, `lock.provider`, `.class`, `.impl`, `.factory`, `classname`, `.url` or `.endpoint` are not allowed. "
        + "Within the `fs.*` prefixes, keys ending with `.impl`, `.class`, `.classname` or `.factory`, and keys containing `provider`, `signers`, `interceptors`, `binding`, `keyfile` or `keytab`, are not allowed; "
        + "the credential and endpoint keys `access.key`, `secret.key`, `session.token`, `endpoint`, `path.style.access` and `region` (e.g. `fs.s3a.access.key`) are always accepted. "
        + "`fs.defaultFS` and `fs.viewfs.*` are not allowed, the scheme comes from `basePath`.";

    @Schema(
        title = "The base path of the Hudi table",
        description = BASE_PATH_DESCRIPTION
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
        description = CONFIGURATION_DESCRIPTION
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

    private static final List<String> ALLOWED_KEY_PREFIXES = List.of("hoodie.", "fs.s3a.", "fs.gs.", "fs.azure.", "fs.abfs.");
    private static final List<String> STORAGE_KEY_PREFIXES = List.of("fs.s3a.", "fs.gs.", "fs.azure.", "fs.abfs.");

    /** Small deny set inside {@code hoodie.}: keys that name callbacks, lock providers, classes or remote endpoints. */
    private static final List<String> DENIED_HOODIE_FRAGMENTS = List.of(
        "callback", "lock.provider", ".class", ".impl", ".factory", "classname", ".url", ".endpoint", "compaction.strategy"
    );

    /** Key shapes that are not accepted under the storage prefixes, except the explicit keys below. */
    private static final List<String> DENIED_STORAGE_SUFFIXES = List.of(".impl", ".class", ".classname", ".factory", ".provider");
    private static final List<String> DENIED_STORAGE_FRAGMENTS = List.of("provider", "signers", "interceptors", "binding", "keyfile", "keytab");

    /** Credential and endpoint settings, accepted whatever the shape rules above say. */
    private static final List<String> ALLOWED_STORAGE_KEY_NAMES = List.of(
        "access.key", "secret.key", "session.token", "endpoint", "path.style.access", "region", "endpoint.region"
    );

    private static final Pattern SCHEME_IMPL_KEY = Pattern.compile("^fs\\.[^.]+\\.impl$");

    static void validateConfigurationKey(String key) {
        var lower = key == null ? "" : key.toLowerCase(Locale.ROOT);
        if (!isAllowedConfigurationKey(lower)) {
            throw new IllegalArgumentException("Configuration key '" + key + "' is not allowed: see the 'configuration' property documentation for the allowed key prefixes (" + String.join(", ", ALLOWED_KEY_PREFIXES) + ") and remove it from 'configuration'");
        }
    }

    private static boolean isAllowedConfigurationKey(String lower) {
        if (lower.startsWith("fs.viewfs.") || SCHEME_IMPL_KEY.matcher(lower).matches()) {
            return false;
        }

        if (lower.startsWith("hoodie.")) {
            return DENIED_HOODIE_FRAGMENTS.stream().noneMatch(lower::contains);
        }

        var storagePrefix = STORAGE_KEY_PREFIXES.stream().filter(lower::startsWith).findFirst();
        if (storagePrefix.isEmpty()) {
            return false;
        }

        var name = lower.substring(storagePrefix.get().length());
        if (ALLOWED_STORAGE_KEY_NAMES.contains(name)) {
            return true;
        }
        return DENIED_STORAGE_SUFFIXES.stream().noneMatch(lower::endsWith)
            && DENIED_STORAGE_FRAGMENTS.stream().noneMatch(lower::contains);
    }

    /** Remote schemes accepted in {@code basePath}; any other scheme is rejected. */
    static final List<String> ALLOWED_REMOTE_SCHEMES = List.of("hdfs", "s3a", "gs", "abfs", "abfss", "wasb", "wasbs");

    private static final Pattern SCHEME = Pattern.compile("^([a-zA-Z][a-zA-Z0-9+.-]+):");

    private static String renderBasePath(RunContext runContext, Property<String> basePath) throws IllegalVariableEvaluationException {
        return runContext.render(basePath).as(String.class)
            .filter(v -> !v.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("basePath is required"));
    }

    private static IllegalArgumentException unsupportedScheme(String rendered, String scheme) {
        return new IllegalArgumentException("Unsupported scheme '" + scheme + "' in basePath '" + rendered + "', allowed remote schemes: " + String.join(", ", ALLOWED_REMOTE_SCHEMES));
    }

    /**
     * Renders {@code basePath} for contexts without a working directory (triggers): only the allowed remote schemes are accepted.
     */
    public static String resolveRemoteBasePath(RunContext runContext, Property<String> basePath) throws IllegalVariableEvaluationException {
        var rendered = renderBasePath(runContext, basePath);
        var schemeMatcher = SCHEME.matcher(rendered);
        if (schemeMatcher.find() && ALLOWED_REMOTE_SCHEMES.contains(schemeMatcher.group(1).toLowerCase(Locale.ROOT))) {
            return rendered;
        }
        throw new IllegalArgumentException("basePath '" + rendered + "' is not supported here: a trigger has no shared working directory, so it needs a remote table path, allowed remote schemes: " + String.join(", ", ALLOWED_REMOTE_SCHEMES));
    }

    /**
     * Renders {@code basePath}. Allowed remote schemes are returned unchanged; local paths (scheme-less or {@code file:})
     * are resolved against the working directory and must stay inside it once normalized and symlinks are resolved.
     */
    public static String resolveBasePath(RunContext runContext, Property<String> basePath) throws IllegalVariableEvaluationException, IOException {
        var rendered = renderBasePath(runContext, basePath);

        var schemeMatcher = SCHEME.matcher(rendered);
        String localPath;
        if (schemeMatcher.find()) {
            var scheme = schemeMatcher.group(1).toLowerCase(Locale.ROOT);
            if (ALLOWED_REMOTE_SCHEMES.contains(scheme)) {
                return rendered;
            }
            if (!"file".equals(scheme)) {
                throw unsupportedScheme(rendered, scheme);
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
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
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
