package info.archinnov.achilles.statement.cache;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.statement.CQLPreparedStatementGenerator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

/**
 * CacheManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class CacheManager
{
    private CQLPreparedStatementGenerator generator = new CQLPreparedStatementGenerator();

    private Function<PropertyMeta<?, ?>, String> propertyExtractor = new Function<PropertyMeta<?, ?>, String>()
    {
        @Override
        public String apply(PropertyMeta<?, ?> pm)
        {
            return pm.getPropertyName();
        }
    };

    public PreparedStatement getCacheForFieldSelect(Session session,
            Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
            CQLPersistenceContext context, PropertyMeta<?, ?> pm)
    {
        Class<?> entityClass = context.getEntityClass();
        EntityMeta entityMeta = context.getEntityMeta();
        Set<String> clusteredFields = extractClusteredFieldsIfNecessary(pm);
        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.SELECT_FIELD,
                entityMeta.getTableName(), clusteredFields, entityClass);
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null)
        {
            ps = generator.prepareSelectFieldPS(session, entityMeta, pm);
            dynamicPSCache.put(cacheKey, ps);
        }
        return ps;
    }

    public PreparedStatement getCacheForFieldsUpdate(Session session,
            Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
            CQLPersistenceContext context, List<PropertyMeta<?, ?>> pms)
    {
        Class<?> entityClass = context.getEntityClass();
        EntityMeta entityMeta = context.getEntityMeta();
        Set<String> fields = new HashSet<String>(Collections2.transform(pms, propertyExtractor));
        StatementCacheKey cacheKey = new StatementCacheKey(CacheType.UPDATE_FIELDS,
                entityMeta.getTableName(), fields, entityClass);
        PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
        if (ps == null)
        {
            ps = generator.prepareUpdateFields(session, entityMeta, pms);
            dynamicPSCache.put(cacheKey, ps);
        }
        return ps;
    }

    private Set<String> extractClusteredFieldsIfNecessary(PropertyMeta<?, ?> pm)
    {
        if (pm.isCompound())
        {
            return new HashSet<String>(pm.getCQLComponentNames());
        }
        else
        {
            return Sets.newHashSet(pm.getPropertyName());
        }
    }
}
