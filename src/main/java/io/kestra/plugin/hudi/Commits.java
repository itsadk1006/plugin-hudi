package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.Builder;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "List timeline commits for a Hudi table",
            code = {
                "id: hudi_commits",
                "namespace: company.team",
                "type: io.kestra.plugin.hudi.Commits",
                "basePath: s3a://my-lake/tables/orders",
                "tableName: orders",
                "fetchType: FETCH"
            }
        )
    }
)
@Schema(
    title = "List timeline commits for an Apache Hudi table."
)
public class Commits extends AbstractHudiConnection implements RunnableTask<Commits.Output> {

    @Schema(title = "The way you want to fetch the data.")
    @NotNull
    private Property<FetchType> fetchType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String table = runContext.render(tableName).as(String.class).orElse(null);
        logger.info("Listing commits for Hudi table: {}", table);
        
        return Output.builder().size(0L).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of commits.")
        private List<Map<String, Object>> rows;

        @Schema(title = "The URI of the stored data.", description = "Only populated if fetchType is STORE.")
        private String uri;

        @Schema(title = "The number of fetched rows.")
        private Long size;
    }
}
