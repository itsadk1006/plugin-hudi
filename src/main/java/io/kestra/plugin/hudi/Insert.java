package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
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
            title = "Insert records into an Apache Hudi table",
            code = """
                id: hudi_insert_flow
                namespace: company.team

                tasks:
                  - id: insert_records
                    type: io.kestra.plugin.hudi.Insert
                    basePath: hdfs://namenode:8020/data/hudi/orders
                    tableName: orders
                    recordKeyField: order_id
                    from:
                      - order_id: 101
                        customer: John Doe
                        amount: 150.50
                """
        )
    }
)
@Schema(
    title = "Insert data into an Apache Hudi table",
    description = """
        Insert records into an Apache Hudi table using HoodieJavaWriteClient.
        """
)
public class Insert extends AbstractHudiWrite implements RunnableTask<AbstractHudiWrite.Output> {

    @Schema(
        title = "Precombine field",
        description = """
            Field name to determine the latest record when deduplicating.
            """
    )
    @PluginProperty(group = "processing")
    private Property<String> precombineField;

    @Override
    public Output run(RunContext runContext) throws Exception {
        return runWrite(runContext);
    }

    @Override
    protected String operationName() {
        return "insert";
    }

    @Override
    protected List<WriteStatus> write(HoodieJavaWriteClient<HoodieAvroPayload> client, List<HoodieRecord<HoodieAvroPayload>> records, String instantTime) {
        return client.insert(records, instantTime);
    }

    @Override
    protected long writtenRecords(HoodieWriteStat stat) {
        return stat.getNumInserts();
    }
}
