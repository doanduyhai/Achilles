package info.archinnov.achilles.query.typed;

import com.datastax.driver.core.PagingState;

import java.util.List;

public class EntitiesWithPagingState<T> {

    private final List<T> entities;
    private final PagingState pagingState;

    public EntitiesWithPagingState(List<T> entities, PagingState pagingState) {
        this.entities = entities;
        this.pagingState = pagingState;
    }

    public List<T> getEntities() {
        return entities;
    }

    public PagingState getPagingState() {
        return pagingState;
    }
}
