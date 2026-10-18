package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.slf4j.Logger;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Poll for new Apache Hudi commits",
            code = """
                id: hudi_trigger_flow
                namespace: company.team

                tasks:
                  - id: log_commit
                    type: io.kestra.plugin.core.log.Log
                    message: "New commit detected: {{ trigger.lastCommitTime }}"

                triggers:
                  - id: watch_orders
                    type: io.kestra.plugin.hudi.Trigger
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                    interval: PT1M
                """
        )
    }
)
@Schema(
    title = "Wait for new commits in an Apache Hudi table",
    description = "Polls the active timeline of an Apache Hudi table and triggers execution when new commits appear."
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {

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

    @Schema(
        title = "Polling interval",
        description = "The duration between successive checks against the Hudi table timeline."
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private Duration interval = Duration.ofMinutes(5);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();
        String renderedBasePath = runContext.render(this.basePath).as(String.class).orElseThrow();
        String renderedTableName = runContext.render(this.tableName).as(String.class).orElseThrow();
        AbstractHudiConnection.disableNativeIOOnWindows();

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
            Map<String, String> renderedConfig = runContext.render(this.configuration).asMap(String.class, String.class);
            if (renderedConfig != null) {
                renderedConfig.forEach((k, v) -> {
                    if (v != null) {
                        hadoopConf.set(k, v);
                    }
                });
            }
        }

        HoodieTableMetaClient metaClient;
        try {
            metaClient = HoodieTableMetaClient.builder()
                .setConf(new HadoopStorageConfiguration(hadoopConf))
                .setBasePath(renderedBasePath)
                .build();
        } catch (Exception e) {
            logger.debug("Hudi table at '{}' is not initialized or not found: {}", renderedBasePath, e.getMessage());
            return Optional.empty();
        }

        Option<HoodieInstant> latestInstant = metaClient.getActiveTimeline()
            .getCommitsTimeline()
            .filterCompletedInstants()
            .lastInstant();

        if (latestInstant.isEmpty()) {
            logger.debug("No completed commits found on Hudi table '{}'", renderedTableName);
            return Optional.empty();
        }

        String latestCommitTime = latestInstant.get().requestedTime();
        String flowId = conditionContext.getFlow().getId();
        String triggerId = context.getTriggerId();
        String namespace = conditionContext.getFlow().getNamespace();

        String stateKey = "hudi_watermark_"
            + flowId.length() + "_" + flowId + "_"
            + triggerId.length() + "_" + triggerId + "_"
            + renderedTableName.length() + "_" + renderedTableName;

        KVStore kvStore = runContext.namespaceKv(namespace);
        Optional<KVValue> valueOptional = kvStore.getValue(stateKey);
        String watermark = valueOptional.map(kv -> (String) kv.value()).orElse(null);

        if (watermark == null || latestCommitTime.compareTo(watermark) > 0) {
            logger.info("New commit detected: {} for table '{}' (previous watermark: {})", latestCommitTime, renderedTableName, watermark);
            kvStore.put(stateKey, new KVValueAndMetadata(null, latestCommitTime));

            Output output = Output.builder()
                .lastCommitTime(latestCommitTime)
                .action(latestInstant.get().getAction())
                .tableName(renderedTableName)
                .basePath(renderedBasePath)
                .build();

            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
        }

        return Optional.empty();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Latest commit instant timestamp",
            description = "The timestamp of the latest completed commit on the active timeline."
        )
        private String lastCommitTime;

        @Schema(
            title = "Action type",
            description = "The action type of the latest commit (e.g. commit, deltacommit)."
        )
        private String action;

        @Schema(
            title = "Table name",
            description = "The name of the triggered Hudi table."
        )
        private String tableName;

        @Schema(
            title = "Base path",
            description = "The base path of the triggered Hudi table."
        )
        private String basePath;
    }
}
