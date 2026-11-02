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
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.io.FileOutputStream;
import java.util.ArrayList;
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
                    basePath: hdfs://namenode:8020/data/hudi/orders
                    tableName: orders
                    fetchType: FETCH
                """
        )
    }
)
@Schema(
    title = "List timeline commits for an Apache Hudi table",
    description = """
        Read the active timeline of an Apache Hudi table and return commit metadata.
        """
)
public class Commits extends AbstractHudiConnection implements RunnableTask<Commits.Output> {

    @Schema(
        title = "The fetch type",
        description = """
            How to return the fetched commits: FETCH (default, returns list in memory), FETCH_ONE (single row), or STORE (stores as internal storage file).
            """
    )
    @PluginProperty(group = "processing")
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();
        var rBasePath = AbstractHudiConnection.resolveBasePath(runContext, this.basePath);
        var rTableName = runContext.render(this.tableName).as(String.class).orElseThrow(() -> new IllegalArgumentException("tableName is required"));
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        logger.info("Reading timeline commits for Hudi table '{}' at '{}'", rTableName, rBasePath);

        var metaClient = getExistingMetaClient(runContext, rBasePath);
        var activeTimeline = metaClient.getActiveTimeline();
        var completedInstants = activeTimeline.getCommitsTimeline().filterCompletedInstants().getInstants();

        var rows = new ArrayList<Map<String, Object>>();
        for (var instant : completedInstants) {
            var isReplace = HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction())
                || HoodieTimeline.CLUSTERING_ACTION.equals(instant.getAction());
            var commitMetadata = isReplace
                ? activeTimeline.readReplaceCommitMetadata(instant)
                : activeTimeline.readCommitMetadata(instant);

            var row = new LinkedHashMap<String, Object>();
            row.put("instantTime", instant.requestedTime());
            row.put("action", instant.getAction());
            row.put("state", instant.getState().name());
            row.put("totalRecords", commitMetadata.fetchTotalRecordsWritten());
            rows.add(row);
        }

        logger.info("Found {} completed commits for table '{}'", rows.size(), rTableName);

        return switch (rFetchType) {
            case FETCH_ONE -> {
                var single = rows.isEmpty() ? null : rows.getLast();
                yield Output.builder()
                    .row(single)
                    .size(single != null ? 1L : 0L)
                    .build();
            }
            case STORE -> {
                var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var output = new FileOutputStream(tempFile)) {
                    for (var row : rows) {
                        FileSerde.write(output, row);
                    }
                }
                yield Output.builder()
                    .uri(runContext.storage().putFile(tempFile).toString())
                    .size((long) rows.size())
                    .build();
            }
            default -> Output.builder()
                .rows(rows)
                .size((long) rows.size())
                .build();
        };
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of commits",
            description = """
                List of commit metadata objects when fetchType is FETCH.
                """
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "Single commit",
            description = """
                Single (latest) commit metadata object when fetchType is FETCH_ONE.
                """
        )
        private Map<String, Object> row;

        @Schema(
            title = "Internal storage URI",
            description = """
                URI of stored commits file when fetchType is STORE.
                """
        )
        private String uri;

        @Schema(
            title = "Number of fetched commits",
            description = """
                Total number of commits retrieved from the timeline.
                """
        )
        private Long size;
    }
}
