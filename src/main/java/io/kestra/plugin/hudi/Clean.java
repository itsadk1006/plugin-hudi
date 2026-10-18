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
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                """
        )
    }
)
@Schema(
    title = "Clean an Apache Hudi table",
    description = "Trigger clean table service on an Apache Hudi table to remove older file slices."
)
public class Clean extends AbstractHudiConnection implements RunnableTask<Clean.Output> {

    @Builder.Default
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Builder.Default
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<String> runningInstant = new AtomicReference<>();

    @Builder.Default
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicReference<HoodieJavaWriteClient<HoodieAvroPayload>> activeClient = new AtomicReference<>();

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedBasePath = runContext.render(this.basePath).as(String.class).orElseThrow();
        String renderedTableName = runContext.render(this.tableName).as(String.class).orElseThrow();

        logger.info("Cleaning Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        getOrInitMetaClient(runContext, renderedBasePath, renderedTableName, HoodieTableType.COPY_ON_WRITE);

        HoodieWriteConfig writeConfig = baseWriteConfigBuilder(runContext, renderedBasePath, renderedTableName).build();

        try (HoodieJavaWriteClient<HoodieAvroPayload> client = createJavaWriteClient(runContext, writeConfig)) {
            activeClient.set(client);
            if (isKilled.get()) {
                throw new IllegalStateException("Clean task was killed before execution");
            }

            String cleanInstant = client.createNewInstantTime(false);
            runningInstant.set(cleanInstant);

            if (isKilled.get()) {
                throw new IllegalStateException("Clean task was killed during execution");
            }

            HoodieCleanMetadata cleanMetadata = client.clean(cleanInstant);
            long totalFilesCleaned = cleanMetadata != null ? cleanMetadata.getTotalFilesDeleted() : 0L;

            logger.info("Successfully cleaned {} files for instant {} on table '{}'", totalFilesCleaned, cleanInstant, renderedTableName);
            return Output.builder()
                .instantTime(cleanInstant)
                .filesCleaned(totalFilesCleaned)
                .build();
        } finally {
            activeClient.set(null);
            runningInstant.set(null);
        }
    }

    @Override
    public void kill() {
        isKilled.set(true);
        String instant = runningInstant.get();
        HoodieJavaWriteClient<HoodieAvroPayload> client = activeClient.get();
        if (client != null && instant != null) {
            try {
                client.rollback(instant);
            } catch (Exception ignored) {
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Clean instant timestamp",
            description = "The timestamp of the executed clean instant."
        )
        private String instantTime;

        @Schema(
            title = "Number of files cleaned",
            description = "The total number of file slices deleted by the clean service."
        )
        private Long filesCleaned;
    }
}
