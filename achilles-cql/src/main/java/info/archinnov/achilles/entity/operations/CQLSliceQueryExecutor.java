package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLImmediateFlushContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.iterator.CQLSliceQueryIterator;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Row;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * CQLSliceQueryExecutor
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLSliceQueryExecutor extends SliceQueryExecutor<CQLPersistenceContext> {

    private CQLStatementGenerator generator = new CQLStatementGenerator();
    private ReflectionInvoker invoker = new ReflectionInvoker();
    private CQLEntityMapper mapper = new CQLEntityMapper();
    private CQLDaoContext daoContext;
    private ConfigurationContext configContext;

    public CQLSliceQueryExecutor(ConfigurationContext configContext, CQLDaoContext daoContext) {
        super(new CQLEntityProxifier());
        this.daoContext = daoContext;
        this.configContext = configContext;
        defaultReadLevel = configContext.getConsistencyPolicy().getDefaultGlobalReadConsistencyLevel();
    }

    @Override
    public <T> List<T> get(SliceQuery<T> sliceQuery) {

        Class<T> entityClass = sliceQuery.getEntityClass();
        EntityMeta meta = sliceQuery.getMeta();

        List<T> clusteredEntities = new ArrayList<T>();

        CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<T>(sliceQuery, defaultReadLevel);
        Query query = generator.generateSelectSliceQuery(cqlSliceQuery, cqlSliceQuery.getLimit());
        List<Row> rows = daoContext.execute(query).all();

        for (Row row : rows)
        {
            T clusteredEntity = invoker.instanciate(entityClass);
            mapper.setEagerPropertiesToEntity(row, meta, clusteredEntity);
            clusteredEntities.add(clusteredEntity);
        }

        return Lists.transform(clusteredEntities, getProxyTransformer(sliceQuery, meta.getEagerGetters()));
    }

    @Override
    public <T> Iterator<T> iterator(SliceQuery<T> sliceQuery) {

        CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<T>(sliceQuery, defaultReadLevel);
        Query query = generator.generateSelectSliceQuery(cqlSliceQuery, cqlSliceQuery.getBatchSize());
        Iterator<Row> iterator = daoContext.execute(query).iterator();
        PreparedStatement ps = generator.generateIteratorSliceQuery(cqlSliceQuery, daoContext);
        CQLPersistenceContext context = buildContextForQuery(sliceQuery);
        return new CQLSliceQueryIterator<T>(cqlSliceQuery, context, iterator, ps);
    }

    @Override
    public <T> void remove(SliceQuery<T> sliceQuery) {
        CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<T>(sliceQuery, defaultReadLevel);
        cqlSliceQuery.validateSliceQueryForRemove();
        Query query = generator.generateRemoveSliceQuery(cqlSliceQuery);
        daoContext.execute(query);
    }

    @Override
    protected <T> CQLPersistenceContext buildContextForQuery(SliceQuery<T> sliceQuery)
    {
        ConsistencyLevel cl = sliceQuery.getConsistencyLevel() == null ? defaultReadLevel : sliceQuery
                .getConsistencyLevel();
        CQLImmediateFlushContext flushContext = new CQLImmediateFlushContext(daoContext,
                Optional.fromNullable(cl), Optional.fromNullable(cl), NO_TTL);

        Object partitionKey = sliceQuery.getPartitionKey();
        EntityMeta meta = sliceQuery.getMeta();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();

        Object embeddedId = invoker.instanciateEmbeddedIdWithPartitionKey(idMeta, partitionKey);
        return new CQLPersistenceContext(meta, configContext, daoContext, flushContext, sliceQuery.getEntityClass(),
                embeddedId, new HashSet<String>());
    }

    @Override
    protected <T> CQLPersistenceContext buildNewContext(SliceQuery<T> sliceQuery, T clusteredEntity)
    {
        EntityMeta meta = sliceQuery.getMeta();
        CQLImmediateFlushContext flushContext = new CQLImmediateFlushContext(daoContext,
                NO_CONSISTENCY_LEVEL, NO_CONSISTENCY_LEVEL, NO_TTL);

        return new CQLPersistenceContext(meta, configContext, daoContext, flushContext,
                clusteredEntity, new HashSet<String>());
    }

}
