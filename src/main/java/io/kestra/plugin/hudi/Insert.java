package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Insert records into a Hudi table",
            code = {
                "id: hudi_insert",
                "namespace: company.team",
                "type: io.kestra.plugin.hudi.Insert",
                "basePath: s3a://my-lake/tables/orders",
                "tableName: orders",
                "from:",
                "  - order_id: 2",
                "    status: 'PENDING'"
            }
        )
    }
)
@Schema(
    title = "Insert data into an Apache Hudi table."
)
public class Insert extends AbstractHudiConnection implements RunnableTask<Insert.Output> {

    @Schema(title = "The data to insert. Can be a Kestra internal storage URI or a list of maps.")
    @NotNull
    private Property<Object> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String table = runContext.render(tableName).as(String.class).orElse(null);
        logger.info("Inserting to Hudi table: {}", table);
        
        // Hudi insert logic would be implemented here.
        
        return Output.builder().recordCount(0L).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of records inserted.")
        private Long recordCount;
    }
}
