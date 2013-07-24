package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLImmediateFlushContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.CQLStatementGenerator;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
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
    private Session session;
    private CQLDaoContext daoContext;
    private ConfigurationContext configContext;

    public CQLSliceQueryExecutor(Session session, CQLDaoContext daoContext, ConfigurationContext configContext) {
        super(new CQLEntityProxifier());
        this.session = session;
        this.daoContext = daoContext;
        this.configContext = configContext;
    }

    @Override
    public <T> List<T> get(SliceQuery<T> sliceQuery) {

        Class<T> entityClass = sliceQuery.getEntityClass();
        EntityMeta meta = sliceQuery.getMeta();

        List<T> clusteredEntities = new ArrayList<T>();

        CQLSliceQuery<T> cqlSliceQuery = new CQLSliceQuery<T>(sliceQuery);
        Query query = generator.generateSliceQuery(cqlSliceQuery);
        List<Row> rows = session.execute(query).all();

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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> void remove(SliceQuery<T> sliceQuery) {
        // TODO Auto-generated method stub

    }

    @Override
    protected <T> CQLPersistenceContext buildNewContext(SliceQuery<T> sliceQuery, T clusteredEntity)
    {
        EntityMeta meta = sliceQuery.getMeta();
        ConsistencyLevel consistencyLevel = sliceQuery.getConsistencyLevel();

        CQLImmediateFlushContext flushContext = new CQLImmediateFlushContext(daoContext,
                Optional.fromNullable(consistencyLevel), NO_CONSISTENCY_LEVEL, NO_TTL);
        return new CQLPersistenceContext(meta, configContext, daoContext, flushContext,
                clusteredEntity, new HashSet<String>());
    }

}
