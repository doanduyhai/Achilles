package info.archinnov.achilles.context;

import info.archinnov.achilles.type.ConsistencyLevel;
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

    public PersistenceContext newContext(Object entity, Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO);

    public PersistenceContext newContext(Object entity);

    public PersistenceContext newContext(Class<?> entityClass, Object primaryKey,
            Optional<ConsistencyLevel> readLevelO,
            Optional<ConsistencyLevel> writeLevelO,
            Optional<Integer> ttlO);

    public PersistenceContext newContextForSliceQuery(Class<?> entityClass, Object partitionKey,
            ConsistencyLevel cl);

}
