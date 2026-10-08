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
            title = "Delete records from a Hudi table",
            code = {
                "id: hudi_delete",
                "namespace: company.team",
                "type: io.kestra.plugin.hudi.Delete",
                "basePath: s3a://my-lake/tables/orders",
                "tableName: orders",
                "recordKeyField: order_id",
                "from:",
                "  - order_id: 1"
            }
        )
    }
)
@Schema(
    title = "Delete data from an Apache Hudi table."
)
public class Delete extends AbstractHudiConnection implements RunnableTask<Delete.Output> {

    @Schema(title = "The record key field.")
    @NotNull
    private Property<String> recordKeyField;

    @Schema(title = "The data to delete. Can be a Kestra internal storage URI or a list of maps containing the record keys.")
    @NotNull
    private Property<Object> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String table = runContext.render(tableName).as(String.class).orElse(null);
        logger.info("Deleting from Hudi table: {}", table);
        
        // Hudi delete logic would be implemented here.
        
        return Output.builder().recordCount(0L).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of records deleted.")
        private Long recordCount;
    }
}
