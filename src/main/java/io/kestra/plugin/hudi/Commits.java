package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
            title = "List timeline commits for an Apache Hudi table",
            code = """
                id: hudi_commits_flow
                namespace: company.team

                tasks:
                  - id: list_commits
                    type: io.kestra.plugin.hudi.Commits
                    basePath: file:///tmp/hudi_tables/orders
                    tableName: orders
                    fetchType: FETCH
                """
        )
    }
)
@Schema(
    title = "List timeline commits for an Apache Hudi table",
    description = "Read the active timeline of an Apache Hudi table and return commit metadata."
)
public class Commits extends AbstractHudiConnection implements RunnableTask<Commits.Output> {

    @Schema(
        title = "The fetch type",
        description = "How to return the fetched commits: FETCH (default, returns list in memory), FETCH_ONE (single row), or STORE (stores as internal storage file)."
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedBasePath = runContext.render(this.basePath).as(String.class).orElseThrow();
        String renderedTableName = runContext.render(this.tableName).as(String.class).orElseThrow();
        FetchType renderedFetchType = this.fetchType != null ? runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH) : FetchType.FETCH;

        logger.info("Reading timeline commits for Hudi table '{}' at '{}'", renderedTableName, renderedBasePath);

        HoodieTableMetaClient metaClient = getOrInitMetaClient(runContext, renderedBasePath, renderedTableName, HoodieTableType.COPY_ON_WRITE);
        HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
        List<HoodieInstant> completedInstants = activeTimeline.getCommitsTimeline().filterCompletedInstants().getInstants();

        List<Map<String, Object>> rows = new ArrayList<>();
        for (HoodieInstant instant : completedInstants) {
            String instantTime = instant.requestedTime();
            String action = instant.getAction();
            String state = instant.getState().name();
            long totalRecords = 0L;

            try {
                Option<byte[]> details = activeTimeline.getInstantDetails(instant);
                if (details.isPresent()) {
                    String json = new String(details.get(), StandardCharsets.UTF_8);
                    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromJsonString(json, HoodieCommitMetadata.class);
                    totalRecords = commitMetadata.fetchTotalRecordsWritten();
                }
            } catch (Exception e) {
                logger.debug("Could not parse commit metadata for instant {}", instantTime, e);
            }

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("instantTime", instantTime);
            row.put("action", action);
            row.put("state", state);
            row.put("totalRecords", totalRecords);
            rows.add(row);
        }

        logger.info("Found {} completed commits for table '{}'", rows.size(), renderedTableName);

        if (renderedFetchType == FetchType.FETCH_ONE) {
            Map<String, Object> single = rows.isEmpty() ? null : rows.get(rows.size() - 1);
            return Output.builder()
                .rows(single != null ? List.of(single) : Collections.emptyList())
                .row(single)
                .size(single != null ? 1L : 0L)
                .build();
        } else if (renderedFetchType == FetchType.STORE) {
            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
            try (OutputStream output = new FileOutputStream(tempFile)) {
                for (Map<String, Object> row : rows) {
                    FileSerde.write(output, row);
                }
            }
            URI uri = runContext.storage().putFile(tempFile);
            return Output.builder()
                .uri(uri.toString())
                .size((long) rows.size())
                .build();
        } else {
            return Output.builder()
                .rows(rows)
                .size((long) rows.size())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of commits",
            description = "List of commit metadata objects."
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Single commit",
            description = "Single commit metadata object when fetchType is FETCH_ONE."
        )
        private Map<String, Object> row;

        @Schema(
            title = "Internal storage URI",
            description = "URI of stored commits file when fetchType is STORE."
        )
        private String uri;

        @Schema(
            title = "Number of fetched commits",
            description = "Total number of commits retrieved from the timeline."
        )
        private Long size;
    }
}
