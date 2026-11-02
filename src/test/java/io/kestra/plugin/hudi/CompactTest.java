package io.kestra.plugin.hudi;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class CompactTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static Compact compact(String basePath) {
        return Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_mor"))
            .build();
    }

    private static void initMorTable(String basePath) throws Exception {
        HudiTestUtils.initTable(basePath, "orders_mor", HoodieTableType.MERGE_ON_READ);
    }

    @Test
    void runCompact() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        initMorTable(basePath);

        var runOutput = compact(basePath).run(runContext);

        assertThat(runOutput).isNotNull();
        assertThat(runOutput.getInstantTime()).isNull();
    }

    @Test
    void compactsLogFilesIntoNewBaseFiles() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        HudiTestUtils.initTable(basePath, "orders_mor", HoodieTableType.MERGE_ON_READ);
        HudiTestUtils.upsert(runContext, basePath, "orders_mor", List.of(
            Map.of("id", "1", "ts", 1000L, "val", "old"),
            Map.of("id", "2", "ts", 1000L, "val", "old")
        ));
        HudiTestUtils.upsert(runContext, basePath, "orders_mor", List.of(Map.of("id", "1", "ts", 2000L, "val", "new")));

        // the update sits in a log file, the base file still holds the old value
        assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().countInstants()).isEqualTo(2);
        assertThat(HudiTestUtils.readLatestRecords(basePath)).extracting(r -> HudiTestUtils.field(r, "val")).containsOnly("old");

        var task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_mor"))
            .configuration(Property.ofValue(Map.of("hoodie.compact.inline.max.delta.commits", "1")))
            .build();
        var runOutput = task.run(runContext);

        assertThat(runOutput.getInstantTime()).isNotBlank();
        var metaClient = HudiTestUtils.metaClient(basePath);
        assertThat(metaClient.getActiveTimeline().filterPendingCompactionTimeline().countInstants()).isZero();
        assertThat(metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().getInstantsAsStream()
            .map(i -> i.requestedTime())).contains(runOutput.getInstantTime());
        assertThat(HudiTestUtils.latestBaseFileInstants(basePath)).containsOnly(runOutput.getInstantTime());
        assertThat(HudiTestUtils.readLatestRecords(basePath))
            .extracting(r -> HudiTestUtils.field(r, "id") + "=" + HudiTestUtils.field(r, "val"))
            .containsExactlyInAnyOrder("1=new", "2=old");
    }

    @Test
    void copyOnWriteTableHasNothingToCompact() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        HudiTestUtils.initTable(basePath, "orders_mor", HoodieTableType.COPY_ON_WRITE);

        var runOutput = compact(basePath).run(runContext);

        assertThat(runOutput.getInstantTime()).isNull();
    }

    @Test
    void failsOnMissingTable() {
        var runContext = runContextFactory.of();
        var task = compact(HudiTestUtils.tableUri(runContext));

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No Hudi table at");
        assertThat(HudiTestUtils.tablePath(runContext).resolve(".hoodie")).doesNotExist();
    }

    @Test
    void testKillCancellation() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        initMorTable(basePath);

        var task = compact(basePath);
        task.kill();

        assertThatThrownBy(() -> task.run(runContext)).isInstanceOf(IllegalStateException.class);
        assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().getInstants()).isEmpty();
    }

    @Test
    void executesAPlanLeftPendingByAKilledRun() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        HudiTestUtils.initTable(basePath, "orders_mor", HoodieTableType.MERGE_ON_READ);
        HudiTestUtils.upsert(runContext, basePath, "orders_mor", List.of(Map.of("id", "1", "ts", 1000L, "val", "old")));
        HudiTestUtils.upsert(runContext, basePath, "orders_mor", List.of(Map.of("id", "1", "ts", 2000L, "val", "new")));

        var task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_mor"))
            .configuration(Property.ofValue(Map.of("hoodie.compact.inline.max.delta.commits", "1")))
            .build();
        String scheduled;
        try (var client = task.createJavaWriteClient(runContext, task.baseWriteConfigBuilder(runContext, basePath, "orders_mor").build())) {
            scheduled = client.scheduleCompaction(Option.empty()).get();
        }
        // what a run killed right after scheduling leaves behind: a plan, no inflight instant
        assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().filterPendingCompactionTimeline().countInstants()).isEqualTo(1);

        var runOutput = task.run(runContext);

        assertThat(runOutput.getInstantTime()).isEqualTo(scheduled);
        assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().filterPendingCompactionTimeline().countInstants()).isZero();
        assertThat(HudiTestUtils.readLatestRecords(basePath)).extracting(r -> HudiTestUtils.field(r, "val")).containsOnly("new");
    }

    @Test
    void leavesAnInflightInstantUntouched() throws Exception {
        var runContext = runContextFactory.of();
        var basePath = HudiTestUtils.tableUri(runContext);
        HudiTestUtils.initTable(basePath, "orders_mor", HoodieTableType.MERGE_ON_READ);
        HudiTestUtils.upsert(runContext, basePath, "orders_mor", List.of(Map.of("id", "1", "ts", 1000L, "val", "old")));
        HudiTestUtils.upsert(runContext, basePath, "orders_mor", List.of(Map.of("id", "1", "ts", 2000L, "val", "new")));

        var task = Compact.builder()
            .id(IdUtils.create())
            .type(Compact.class.getName())
            .basePath(Property.ofValue(basePath))
            .tableName(Property.ofValue("orders_mor"))
            .configuration(Property.ofValue(Map.of("hoodie.compact.inline.max.delta.commits", "1")))
            .build();
        String scheduled;
        try (var client = task.createJavaWriteClient(runContext, task.baseWriteConfigBuilder(runContext, basePath, "orders_mor").build())) {
            scheduled = client.scheduleCompaction(Option.empty()).get();
        }
        // what another engine leaves while it runs the plan: an inflight instant
        var metaClient = HudiTestUtils.metaClient(basePath);
        var requested = metaClient.getActiveTimeline().filterPendingCompactionTimeline().getInstants().getFirst();
        metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(requested);
        assertThat(HudiTestUtils.metaClient(basePath).getActiveTimeline().filterPendingCompactionTimeline().getInstants())
            .singleElement().matches(HoodieInstant::isInflight);

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining(scheduled)
            .hasMessageContaining("inflight");

        var after = HudiTestUtils.metaClient(basePath).getActiveTimeline();
        assertThat(after.filterPendingCompactionTimeline().getInstants()).singleElement()
            .matches(HoodieInstant::isInflight)
            .matches(i -> i.requestedTime().equals(scheduled));
        assertThat(after.getRollbackTimeline().countInstants()).isZero();
    }
}
