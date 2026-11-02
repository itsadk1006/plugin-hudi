package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Upsert records into an Apache Hudi table",
            code = """
                id: hudi_upsert_flow
                namespace: company.team

                tasks:
                  - id: upsert_records
                    type: io.kestra.plugin.hudi.Upsert
                    basePath: hdfs://namenode:8020/data/hudi/orders
                    tableName: orders
                    recordKeyField: order_id
                    precombineField: updated_at
                    from:
                      - order_id: 1
                        updated_at: 1672531199000
                        status: COMPLETED
                """
        ),
        @Example(
            full = true,
            title = "Upsert into a local table and list its commits, sharing the working directory. A local table lives and dies with the working directory of the task.",
            code = """
                id: hudi_local_flow
                namespace: company.team

                tasks:
                  - id: workdir
                    type: io.kestra.plugin.core.flow.WorkingDirectory
                    tasks:
                      - id: upsert_records
                        type: io.kestra.plugin.hudi.Upsert
                        basePath: hudi_tables/orders
                        tableName: orders
                        recordKeyField: order_id
                        precombineField: updated_at
                        from:
                          - order_id: 1
                            updated_at: 1672531199000
                            status: COMPLETED

                      - id: list_commits
                        type: io.kestra.plugin.hudi.Commits
                        basePath: hudi_tables/orders
                        tableName: orders
                        fetchType: FETCH
                """
        )
    }
)
@Schema(
    title = "Upsert data into an Apache Hudi table",
    description = """
        Upsert records into an Apache Hudi table using HoodieJavaWriteClient.
        """
)
public class Upsert extends AbstractHudiWrite implements RunnableTask<AbstractHudiWrite.Output> {

    @Schema(
        title = "Precombine field",
        description = """
            Field name to determine the latest record when deduplicating.
            """
    )
    @PluginProperty(group = "processing")
    @NotNull
    private Property<String> precombineField;

    @Override
    public Output run(RunContext runContext) throws Exception {
        return runWrite(runContext);
    }

    @Override
    protected String operationName() {
        return "upsert";
    }

    @Override
    protected List<WriteStatus> write(HoodieJavaWriteClient<HoodieAvroPayload> client, List<HoodieRecord<HoodieAvroPayload>> records, String instantTime) {
        return client.upsert(records, instantTime);
    }

    @Override
    protected long writtenRecords(HoodieWriteStat stat) {
        return stat.getNumInserts() + stat.getNumUpdateWrites();
    }
}
