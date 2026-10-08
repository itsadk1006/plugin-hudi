package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHudiConnection extends Task {
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
}
