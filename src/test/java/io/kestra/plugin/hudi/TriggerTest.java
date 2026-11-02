package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    private Trigger trigger(String basePath, String tableName) {
        return Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue(tableName))
            .build();
    }

    @Test
    void runTriggerPolling() throws Exception {
        var basePath = "table";
        var tableName = "orders_trigger";
        var trigger = trigger(basePath, tableName);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue())).isEmpty();

        var upsertOutput = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(HudiTestUtils.tableUri(context.getKey().getRunContext())))
            .tableName(Property.ofValue(tableName))
            .recordKeyField(Property.ofValue("id"))
            .precombineField(Property.ofValue("ts"))
            .from(Property.ofValue(List.of(Map.of("id", "1", "ts", 1000L, "val", "A"))))
            .build()
            .run(context.getKey().getRunContext());
        assertThat(upsertOutput.getCommitInstantTime()).isNotBlank();

        var execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution).isPresent();
        assertThat(execution.get().getTrigger().getVariables())
            .containsEntry("lastCommitTime", upsertOutput.getCommitInstantTime())
            .containsEntry("tableName", tableName);

        assertThat(trigger.evaluate(context.getKey(), context.getValue())).isEmpty();
    }

    @Test
    void failsOnUnsupportedFileSystem() throws Exception {
        var trigger = trigger("unknownscheme://bucket/orders", "orders_trigger");
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThatThrownBy(() -> trigger.evaluate(context.getKey(), context.getValue()))
            .hasMessageContaining("unknownscheme");
    }
}
