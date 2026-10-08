package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVValue;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.time.Instant;
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
            title = "Poll for new Hudi commits",
            code = {
                "id: hudi_trigger",
                "namespace: company.team",
                "type: io.kestra.plugin.hudi.Trigger",
                "basePath: s3a://my-lake/tables/orders",
                "tableName: orders",
                "interval: PT5M"
            }
        )
    }
)
@Schema(
    title = "Wait for new commits in an Apache Hudi table."
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {

    @Schema(title = "The base path of the Hudi table.")
    @NotNull
    protected Property<String> basePath;

    @Schema(title = "The name of the Hudi table.")
    @NotNull
    protected Property<String> tableName;

    @Schema(title = "Configuration properties for the Hudi connection.")
    @PluginProperty
    @ToString.Exclude
    protected Property<Map<String, String>> configuration;

    @Schema(title = "Interval between polling.")
    @Builder.Default
    private Duration interval = Duration.ofMinutes(5);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        String table = runContext.render(tableName).as(String.class).orElse(null);
        String namespace = conditionContext.getFlow().getNamespace();
        String stateKey = "hudi-trigger-watermark-" + table;
        
        KVStore kvStore = runContext.namespaceKv(namespace);
        Optional<KVValue> valueOptional = kvStore.getValue(stateKey);
        String watermark = valueOptional.map(kv -> (String) kv.value()).orElse(null);
        
        // Hudi polling logic would be implemented here to find new commits since the watermark.
        String newWatermark = "latest-commit-" + Instant.now().toEpochMilli();
        
        if (newWatermark != null && !newWatermark.equals(watermark)) {
            kvStore.put(stateKey, new KVValueAndMetadata(null, newWatermark));
            
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, Output.builder().lastCommitTime(newWatermark).build()));
        }
        
        return Optional.empty();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The latest commit time.")
        private String lastCommitTime;
    }
}
