package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Clean an Apache Hudi table",
            code = """
                id: hudi_clean_flow
                namespace: company.team

                tasks:
                  - id: clean_table
                    type: io.kestra.plugin.hudi.Clean
                    basePath: hdfs://namenode:8020/data/hudi/orders
                    tableName: orders
                """
        )
    }
)
@Schema(
    title = "Clean an Apache Hudi table",
    description = """
        Trigger clean table service on an Apache Hudi table to remove older file slices.
        """
)
public class Clean extends AbstractHudiMaintenance implements RunnableTask<Clean.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rBasePath = AbstractHudiConnection.resolveBasePath(runContext, this.basePath);
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));

        logger.info("Cleaning Hudi table '{}' at '{}'", rTableName, rBasePath);

        getExistingMetaClient(runContext, rBasePath);

        var writeConfig = baseWriteConfigBuilder(runContext, rBasePath, rTableName).build();

        try (var client = createJavaWriteClient(runContext, writeConfig)) {
            ensureNotKilled("before execution");

            var cleanInstant = client.createNewInstantTime(false);

            ensureNotKilled("during execution");

            var cleanMetadata = client.clean(cleanInstant);
            var totalFilesCleaned = cleanMetadata != null ? cleanMetadata.getTotalFilesDeleted() : 0L;

            logger.info("Successfully cleaned {} files for instant {} on table '{}'", totalFilesCleaned, cleanInstant, rTableName);
            return Output.builder()
                .instantTime(cleanInstant)
                .filesCleaned(totalFilesCleaned)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Clean instant timestamp",
            description = """
                The timestamp of the executed clean instant.
                """
        )
        private String instantTime;

        @Schema(
            title = "Number of files cleaned",
            description = """
                The total number of file slices deleted by the clean service.
                """
        )
        private Long filesCleaned;
    }
}
