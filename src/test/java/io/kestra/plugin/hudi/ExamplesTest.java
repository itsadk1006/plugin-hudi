package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.serializers.YamlParser;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class ExamplesTest {

    @Inject
    private ModelValidator modelValidator;

    @Test
    void examplesAreValid() {
        var pluginClasses = List.of(Upsert.class, Insert.class, Delete.class, Compact.class, Cluster.class, Clean.class, Commits.class, Trigger.class);

        for (var pluginClass : pluginClasses) {
            var plugin = pluginClass.getAnnotation(Plugin.class);
            assertThat(plugin).as(pluginClass.getSimpleName()).isNotNull();
            assertThat(plugin.examples()).as(pluginClass.getSimpleName()).isNotEmpty();

            for (var example : plugin.examples()) {
                var flow = YamlParser.parse(String.join("\n", example.code()), Flow.class);
                assertThat(modelValidator.isValid(flow)).as(pluginClass.getSimpleName()).isEmpty();
            }
        }
    }
}
