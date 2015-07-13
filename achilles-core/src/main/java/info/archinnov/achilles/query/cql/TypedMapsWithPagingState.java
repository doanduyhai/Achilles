package info.archinnov.achilles.query.cql;

import com.datastax.driver.core.PagingState;
import info.archinnov.achilles.type.TypedMap;

import java.util.List;

public class TypedMapsWithPagingState {

    private final List<TypedMap> typedMaps;
    private final PagingState pagingState;

    public TypedMapsWithPagingState(List<TypedMap> typedMaps, PagingState pagingState) {
        this.typedMaps = typedMaps;
        this.pagingState = pagingState;
    }

    public List<TypedMap> getTypedMaps() {
        return typedMaps;
    }

    public PagingState getPagingState() {
        return pagingState;
    }
}
