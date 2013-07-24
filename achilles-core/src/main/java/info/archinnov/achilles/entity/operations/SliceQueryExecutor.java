package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * SliceQueryExecutor
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class SliceQueryExecutor<CONTEXT extends PersistenceContext> {

    public static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional
            .<ConsistencyLevel> absent();
    public static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

    protected EntityProxifier<CONTEXT> proxifier;

    protected SliceQueryExecutor(EntityProxifier<CONTEXT> proxifier) {
        this.proxifier = proxifier;
    }

    public abstract <T> List<T> get(SliceQuery<T> sliceQuery);

    public abstract <T> Iterator<T> iterator(SliceQuery<T> sliceQuery);

    public abstract <T> void remove(SliceQuery<T> sliceQuery);

    protected abstract <T> CONTEXT buildNewContext(SliceQuery<T> sliceQuery, T clusteredEntity);

    protected <T> Function<T, T> getProxyTransformer(final SliceQuery<T> sliceQuery, final List<Method> getters)
    {
        return new Function<T, T>()
        {
            @Override
            public T apply(T clusteredEntity)
            {
                CONTEXT context = buildNewContext(sliceQuery, clusteredEntity);
                return proxifier.buildProxy(clusteredEntity, context, Sets.newHashSet(getters));
            }
        };
    }
}
