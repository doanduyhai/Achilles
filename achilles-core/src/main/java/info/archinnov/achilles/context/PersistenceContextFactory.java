package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import com.google.common.base.Optional;

/**
 * PersistenceContextFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public interface PersistenceContextFactory {

    public static final Optional<ConsistencyLevel> NO_CONSISTENCY_LEVEL = Optional
            .<ConsistencyLevel> absent();
    public static final Optional<Integer> NO_TTL = Optional.<Integer> absent();

    public PersistenceContext newContext(Object entity, Options options);

    public PersistenceContext newContext(Object entity);

    public PersistenceContext newContext(Class<?> entityClass, Object primaryKey, Options options);

    public PersistenceContext newContextForSliceQuery(Class<?> entityClass, Object partitionKey,
            ConsistencyLevel cl);

}
