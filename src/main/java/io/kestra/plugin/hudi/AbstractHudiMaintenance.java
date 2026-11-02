package io.kestra.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.WorkerJobLifecycle;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractHudiMaintenance extends AbstractHudiConnection implements WorkerJobLifecycle {

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    protected final transient AtomicBoolean isKilled = new AtomicBoolean(false);

    protected void ensureNotKilled(String phase) {
        if (isKilled.get()) {
            throw new IllegalStateException(getClass().getSimpleName() + " task was killed " + phase);
        }
    }

    /**
     * Only flags the task: the write client is not thread safe, so nothing is rolled back from this thread. {@code run()}
     * checks the flag between steps. A table service that is already executing runs to completion, so the table stays consistent.
     */
    @Override
    public void kill() {
        isKilled.set(true);
    }
}
