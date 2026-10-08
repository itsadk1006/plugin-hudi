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
            title = "Upsert records into a Hudi table",
            code = {
                "id: hudi_upsert",
                "namespace: company.team",
                "type: io.kestra.plugin.hudi.Upsert",
                "basePath: s3a://my-lake/tables/orders",
                "tableName: orders",
                "recordKeyField: order_id",
                "precombineField: updated_at",
                "configuration:",
                "  fs.s3a.access.key: \"{{ secret('AWS_ACCESS_KEY_ID') }}\"",
                "from:",
                "  - order_id: 1",
                "    updated_at: '2023-01-01'",
                "    status: 'COMPLETED'"
            }
        )
    }
)
@Schema(
    title = "Upsert data into an Apache Hudi table."
)
public class Upsert extends AbstractHudiConnection implements RunnableTask<Upsert.Output> {

    @Schema(title = "The record key field.")
    @NotNull
    private Property<String> recordKeyField;

    @Schema(title = "The precombine field.")
    @NotNull
    private Property<String> precombineField;

    @Schema(title = "The data to insert/update. Can be a Kestra internal storage URI or a list of maps.")
    @NotNull
    private Property<Object> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String table = runContext.render(tableName).as(String.class).orElse(null);
        logger.info("Upserting to Hudi table: {}", table);
        
        // Hudi upsert logic would be implemented here using hudi-java-client.
        
        return Output.builder().recordCount(0L).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of records upserted.")
        private Long recordCount;
    }
}
