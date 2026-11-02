package io.kestra.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.WorkerJobLifecycle;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHudiMaintenance extends AbstractHudiConnection implements WorkerJobLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHudiMaintenance.class);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    protected final transient AtomicBoolean isKilled = new AtomicBoolean(false);

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    protected final transient AtomicReference<String> runningInstant = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    protected final transient AtomicReference<HoodieJavaWriteClient<HoodieAvroPayload>> activeClient = new AtomicReference<>();

    // A clean instant cannot be rolled back, so Clean opts out
    protected boolean rollbackOnKill() {
        return true;
    }

    protected void ensureNotKilled(String phase) {
        if (isKilled.get()) {
            throw new IllegalStateException(getClass().getSimpleName() + " task was killed " + phase);
        }
    }

    protected void rollbackIfKilled(HoodieJavaWriteClient<HoodieAvroPayload> client, String instant) {
        if (isKilled.get()) {
            client.rollback(instant);
            throw new IllegalStateException(getClass().getSimpleName() + " task was killed during execution");
        }
    }

    @Override
    public void kill() {
        if (!isKilled.compareAndSet(false, true) || !rollbackOnKill()) {
            return;
        }

        var instant = runningInstant.get();
        var client = activeClient.get();
        if (client != null && instant != null) {
            try {
                client.rollback(instant);
            } catch (Exception e) {
                LOGGER.warn("Failed to roll back instant {} after kill", instant, e);
            }
        }
    }
}
