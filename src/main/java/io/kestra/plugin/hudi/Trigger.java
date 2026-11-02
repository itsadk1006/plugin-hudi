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
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

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
                    basePath: hdfs://namenode:8020/data/hudi/orders
                    tableName: orders
                    interval: PT1M
                """
        )
    }
)
@Schema(
    title = "Wait for new commits in an Apache Hudi table",
    description = """
        Polls the active timeline of an Apache Hudi table and triggers execution when new commits appear. The table must be at a remote path, a trigger has no shared working directory.
        """
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {

    @Schema(
        title = "The base path of the Hudi table",
        description = """
            Remote path of the Hudi table (e.g., `hdfs://namenode:8020/data/hudi/orders`). Allowed remote schemes: `hdfs`, `s3a`, `gs`, `abfs`, `abfss`, `wasb`, `wasbs`. Local or `file://` paths are not supported because a trigger has no shared working directory: it is new and empty on every check.
            """
    )
    @PluginProperty(group = "main")
    @NotNull
    protected Property<String> basePath;

    @Schema(
        title = "The name of the Hudi table",
        description = """
            Target Apache Hudi table name.
            """
    )
    @PluginProperty(group = "main")
    @NotNull
    protected Property<String> tableName;

    @Schema(
        title = "Configuration properties for the Hudi connection",
        description = AbstractHudiConnection.CONFIGURATION_DESCRIPTION
    )
    @PluginProperty(group = "connection", secret = true)
    @ToString.Exclude
    protected Property<Map<String, String>> configuration;

    @Schema(
        title = "Polling interval",
        description = """
            The duration between successive checks against the Hudi table timeline.
            """
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private Duration interval = Duration.ofMinutes(5);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var rBasePath = AbstractHudiConnection.resolveRemoteBasePath(conditionContext.getRunContext(), this.basePath);
        return poll(conditionContext, context, rBasePath);
    }

    // Split from evaluate() so the polling logic can be tested without a remote file system.
    Optional<Execution> poll(ConditionContext conditionContext, TriggerContext context, String rBasePath) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));

        var hadoopConf = AbstractHudiConnection.hadoopConfiguration(runContext, this.configuration);
        if (!AbstractHudiConnection.tableExists(hadoopConf, rBasePath)) {
            logger.debug("No Hudi table found at '{}', skipping poll", rBasePath);
            return Optional.empty();
        }

        var metaClient = HoodieTableMetaClient.builder()
            .setConf(new HadoopStorageConfiguration(hadoopConf))
            .setBasePath(rBasePath)
            .build();

        var latestInstant = metaClient.getActiveTimeline()
            .getCommitsTimeline()
            .filterCompletedInstants()
            .lastInstant();

        if (latestInstant.isEmpty()) {
            logger.debug("No completed commits found on Hudi table '{}'", rTableName);
            return Optional.empty();
        }

        var latestCommitTime = latestInstant.get().requestedTime();
        var flowId = conditionContext.getFlow().getId();
        var triggerId = context.getTriggerId();
        var namespace = conditionContext.getFlow().getNamespace();

        var stateKey = "hudi_watermark_"
            + flowId.length() + "_" + flowId + "_"
            + triggerId.length() + "_" + triggerId + "_"
            + rTableName.length() + "_" + rTableName;

        var kvStore = runContext.namespaceKv(namespace);
        var valueOptional = kvStore.getValue(stateKey);
        var watermark = valueOptional.map(kv -> (String) kv.value()).orElse(null);

        if (watermark == null || latestCommitTime.compareTo(watermark) > 0) {
            logger.info("New commit detected: {} for table '{}' (previous watermark: {})", latestCommitTime, rTableName, watermark);
            kvStore.put(stateKey, new KVValueAndMetadata(null, latestCommitTime));

            var output = Output.builder()
                .lastCommitTime(latestCommitTime)
                .action(latestInstant.get().getAction())
                .tableName(rTableName)
                .basePath(rBasePath)
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
            description = """
                The timestamp of the latest completed commit on the active timeline.
                """
        )
        private String lastCommitTime;

        @Schema(
            title = "Action type",
            description = """
                The action type of the latest commit (e.g. commit, deltacommit).
                """
        )
        private String action;

        @Schema(
            title = "Table name",
            description = """
                The name of the triggered Hudi table.
                """
        )
        private String tableName;

        @Schema(
            title = "Base path",
            description = """
                The base path of the triggered Hudi table.
                """
        )
        private String basePath;
    }
}
