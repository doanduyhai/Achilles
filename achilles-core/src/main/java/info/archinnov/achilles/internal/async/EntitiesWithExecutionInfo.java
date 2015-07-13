package info.archinnov.achilles.internal.async;

import com.datastax.driver.core.ExecutionInfo;
import info.archinnov.achilles.query.typed.EntitiesWithPagingState;

import java.util.List;

public class EntitiesWithExecutionInfo<T> {

    private final List<T> entities;
    private final ExecutionInfo executionInfo;

    public EntitiesWithExecutionInfo(List<T> entities, ExecutionInfo executionInfo) {
        this.entities = entities;
        this.executionInfo = executionInfo;
    }

    public List<T> getEntities() {
        return entities;
    }

    public ExecutionInfo getExecutionInfo() {
        return executionInfo;
    }

    public EntitiesWithPagingState<T> toEntitiesWithPagingState() {
        return new EntitiesWithPagingState<>(entities, executionInfo.getPagingState());
    }
}
