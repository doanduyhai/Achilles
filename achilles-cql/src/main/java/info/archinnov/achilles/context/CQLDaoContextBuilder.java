package info.archinnov.achilles.context;

import static info.archinnov.achilles.entity.metadata.EntityMeta.*;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.statement.cache.StatementCacheKey;
import info.archinnov.achilles.statement.prepared.CQLPreparedStatementGenerator;
import java.util.HashMap;
import java.util.Map;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

/**
 * CQLDaoContextBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLDaoContextBuilder
{
    private static final Integer PREPARED_STATEMENT_LRU_CACHE_SIZE = 5000;
    private CQLPreparedStatementGenerator queryGenerator = new CQLPreparedStatementGenerator();
    private Session session;

    private Function<EntityMeta, PreparedStatement> insertPSTransformer = new Function<EntityMeta, PreparedStatement>()
    {
        @Override
        public PreparedStatement apply(EntityMeta meta)
        {
            return queryGenerator.prepareInsertPS(session, meta);
        }
    };

    private Function<EntityMeta, PreparedStatement> selectEagerPSTransformer = new Function<EntityMeta, PreparedStatement>()
    {
        @Override
        public PreparedStatement apply(EntityMeta meta)
        {
            return queryGenerator.prepareSelectEagerPS(session, meta);
        }
    };

    private Function<EntityMeta, Map<String, PreparedStatement>> removePSTransformer = new Function<EntityMeta, Map<String, PreparedStatement>>()
    {
        @Override
        public Map<String, PreparedStatement> apply(EntityMeta meta)
        {
            return queryGenerator.prepareRemovePSs(session, meta);
        }

    };

    private Function<EntityMeta, Map<CQLQueryType, PreparedStatement>> clusteredCounterTransformer = new Function<EntityMeta, Map<CQLQueryType, PreparedStatement>>()
    {
        @Override
        public Map<CQLQueryType, PreparedStatement> apply(EntityMeta meta)
        {
            return queryGenerator.prepareClusteredCounterQueryMap(session, meta);
        }
    };

    public static CQLDaoContextBuilder builder(Session session)
    {
        return new CQLDaoContextBuilder(session);
    }

    public CQLDaoContextBuilder(Session session) {
        this.session = session;
    }

    public CQLDaoContext build(Map<Class<?>, EntityMeta> entityMetaMap, boolean hasSimpleCounter)
    {
        Map<Class<?>, PreparedStatement> insertPSMap = new HashMap<Class<?>, PreparedStatement>(Maps.transformValues(
                Maps.filterValues(entityMetaMap, excludeClusteredCounter), insertPSTransformer));

        Map<Class<?>, PreparedStatement> selectEagerPSMap = new HashMap<Class<?>, PreparedStatement>(
                Maps.transformValues(entityMetaMap, selectEagerPSTransformer));

        Map<Class<?>, Map<String, PreparedStatement>> removePSMap = new HashMap<Class<?>, Map<String, PreparedStatement>>(
                Maps.transformValues(entityMetaMap, removePSTransformer));

        Cache<StatementCacheKey, PreparedStatement> dynamicPSCache = CacheBuilder
                .newBuilder().maximumSize(PREPARED_STATEMENT_LRU_CACHE_SIZE).build();

        Map<CQLQueryType, PreparedStatement> counterQueryMap;
        if (hasSimpleCounter)
        {
            counterQueryMap = queryGenerator.prepareSimpleCounterQueryMap(session);
        }
        else
        {
            counterQueryMap = new HashMap<CQLQueryType, PreparedStatement>();
        }

        Map<Class<?>, Map<CQLQueryType, PreparedStatement>> clusteredCounterQueriesMap = new HashMap<Class<?>, Map<CQLQueryType, PreparedStatement>>(
                Maps.transformValues(
                        Maps.filterValues(entityMetaMap, clusteredCounter), clusteredCounterTransformer));

        return new CQLDaoContext(insertPSMap, dynamicPSCache, selectEagerPSMap, removePSMap,
                counterQueryMap, clusteredCounterQueriesMap, session);
    }
}
