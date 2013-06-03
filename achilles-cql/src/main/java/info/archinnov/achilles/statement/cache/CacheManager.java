package info.archinnov.achilles.statement.cache;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.statement.CQLPreparedStatementGenerator;

import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.Cache;
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

	public PreparedStatement getCacheForFieldSelect(Session session,
			Cache<StatementCacheKey, PreparedStatement> dynamicPSCache,
			CQLPersistenceContext context, PropertyMeta<?, ?> pm)
	{
		Class<?> entityClass = context.getEntityClass();
		EntityMeta entityMeta = context.getEntityMeta();
		Set<String> primaryKeys = extractPrimaryKeys(pm);
		StatementCacheKey cacheKey = new StatementCacheKey(CacheType.SELECT_FIELD,
				entityMeta.getTableName(), primaryKeys, entityClass);
		PreparedStatement ps = dynamicPSCache.getIfPresent(cacheKey);
		if (ps == null)
		{
			ps = generator.prepareSelectFieldPS(session, entityMeta, pm);
			dynamicPSCache.put(cacheKey, ps);
		}
		return ps;
	}

	private Set<String> extractPrimaryKeys(PropertyMeta<?, ?> pm)
	{
		if (pm.isSingleKey())
		{
			return Sets.newHashSet(pm.getPropertyName());
		}
		else
		{
			return new HashSet<String>(pm.getMultiKeyProperties().getComponentNames());
		}
	}
}
