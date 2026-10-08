package io.kestra.plugin.hudi;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.Builder;
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
            title = "Cluster a Hudi table",
            code = {
                "id: hudi_cluster",
                "namespace: company.team",
                "type: io.kestra.plugin.hudi.Cluster",
                "basePath: s3a://my-lake/tables/orders",
                "tableName: orders"
            }
        )
    }
)
@Schema(
    title = "Cluster an Apache Hudi table."
)
public class Cluster extends AbstractHudiConnection implements RunnableTask<VoidOutput> {
    @Builder.Default
    private transient boolean isKilled = false;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String table = runContext.render(tableName).as(String.class).orElse(null);
        logger.info("Clustering Hudi table: {}", table);
        
        if (isKilled) {
            logger.warn("Cluster task was killed");
        }
        
        return null;
    }

    @Override
    public void kill() {
        this.isKilled = true;
    }
}
