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

    // Polling logic only: a real trigger needs a remote table, so the table lives in the working directory of this test.
    @Test
    void runTriggerPolling() throws Exception {
        var basePath = "s3a://my-bucket/hudi/orders";
        var tableName = "orders_trigger";
        var trigger = trigger(basePath, tableName);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var localTable = HudiTestUtils.tableUri(context.getKey().getRunContext());

        assertThat(trigger.poll(context.getKey(), context.getValue(), localTable)).isEmpty();

        var upsertOutput = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(localTable))
            .tableName(Property.ofValue(tableName))
            .recordKeyField(Property.ofValue("id"))
            .precombineField(Property.ofValue("ts"))
            .from(Property.ofValue(List.of(Map.of("id", "1", "ts", 1000L, "val", "A"))))
            .build()
            .run(context.getKey().getRunContext());
        assertThat(upsertOutput.getCommitInstantTime()).isNotBlank();

        var execution = trigger.poll(context.getKey(), context.getValue(), localTable);
        assertThat(execution).isPresent();
        assertThat(execution.get().getTrigger().getVariables())
            .containsEntry("lastCommitTime", upsertOutput.getCommitInstantTime())
            .containsEntry("tableName", tableName);

        assertThat(trigger.poll(context.getKey(), context.getValue(), localTable)).isEmpty();
    }

    // Runs evaluate() on the allowed s3a scheme, served from the local disk by FakeRemoteFileSystem (test core-site.xml).
    @Test
    void evaluateOnARemoteScheme() throws Exception {
        var tableName = "orders_remote";
        var probe = trigger("s3a://fake-bucket/unused", tableName);
        var context = TestsUtils.mockTrigger(runContextFactory, probe);
        var runContext = context.getKey().getRunContext();
        var localPath = HudiTestUtils.tablePath(runContext).toUri().getPath();
        var trigger = trigger("s3a://fake-bucket" + localPath, tableName);

        assertThat(trigger.evaluate(context.getKey(), context.getValue())).isEmpty();

        var upsertOutput = Upsert.builder()
            .id(IdUtils.create())
            .type(Upsert.class.getName())
            .basePath(Property.ofValue(HudiTestUtils.tableUri(runContext)))
            .tableName(Property.ofValue(tableName))
            .recordKeyField(Property.ofValue("id"))
            .precombineField(Property.ofValue("ts"))
            .from(Property.ofValue(List.of(Map.of("id", "1", "ts", 1000L, "val", "A"))))
            .build()
            .run(runContext);

        var execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution).isPresent();
        assertThat(execution.get().getTrigger().getVariables())
            .containsEntry("lastCommitTime", upsertOutput.getCommitInstantTime())
            .containsEntry("basePath", "s3a://fake-bucket" + localPath);

        assertThat(trigger.evaluate(context.getKey(), context.getValue())).isEmpty();
    }

    @Test
    void rejectsLocalPath() throws Exception {
        for (var basePath : List.of("table", "file:///tmp/table")) {
            var trigger = trigger(basePath, "orders_trigger");
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            assertThatThrownBy(() -> trigger.evaluate(context.getKey(), context.getValue()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a remote table path")
                .hasMessageContaining("no shared working directory");
        }
    }

    @Test
    void rejectsSchemeOutsideTheAllowedList() throws Exception {
        var trigger = trigger("viewfs://cluster/orders", "orders_trigger");
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThatThrownBy(() -> trigger.evaluate(context.getKey(), context.getValue()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("allowed remote schemes");
    }
}
