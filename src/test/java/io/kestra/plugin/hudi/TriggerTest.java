package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void setupHadoop() {
        AbstractHudiConnection.disableNativeIOOnWindows();
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            File hadoopHome = new File("C:\\hadoop");
            if (hadoopHome.exists()) {
                System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath());
            }
        }
    }

    @Test
    void runTriggerPolling(@TempDir Path tempDir) throws Exception {
        String basePath = tempDir.toUri().toString();
        String tableName = "orders_trigger";

        Flow flow = Flow.builder()
            .id("test_hudi_flow")
            .namespace("io.kestra.tests")
            .build();

        Trigger trigger = Trigger.builder()
            .id(IdUtils.create())
            .basePath(Property.of(basePath))
            .tableName(Property.of(tableName))
            .build();

        RunContext runContext = runContextFactory.of(flow, trigger);

        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(flow)
            .build();

        TriggerContext triggerContext = TriggerContext.builder()
            .triggerId("test_hudi_trigger")
            .build();

        // 1. Uninitialized table -> Optional.empty()
        Optional<Execution> execution1 = trigger.evaluate(conditionContext, triggerContext);
        assertThat(execution1).isEmpty();

        // 2. Perform an Upsert to produce a commit
        Upsert upsert = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.of(basePath))
            .tableName(Property.of(tableName))
            .recordKeyField(Property.of("id"))
            .precombineField(Property.of("ts"))
            .from(Property.of(List.of(
                Map.of("id", "1", "ts", 1000L, "val", "A")
            )))
            .build();
        Upsert.Output upsertOutput = upsert.run(runContext);
        assertThat(upsertOutput.getCommitInstantTime()).isNotBlank();

        // 3. Trigger evaluates -> should detect new commit
        Optional<Execution> execution2 = trigger.evaluate(conditionContext, triggerContext);
        assertThat(execution2).isPresent();

        // 4. Trigger evaluates again -> watermark prevents re-firing
        Optional<Execution> execution3 = trigger.evaluate(conditionContext, triggerContext);
        assertThat(execution3).isEmpty();
    }
}
