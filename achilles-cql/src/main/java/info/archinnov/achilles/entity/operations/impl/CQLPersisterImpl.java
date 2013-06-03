package info.archinnov.achilles.entity.operations.impl;

import static com.google.common.collect.Collections2.filter;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;

/**
 * CQLPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPersisterImpl
{
	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();

	private Predicate<PropertyMeta<?, ?>> joinFilter = new Predicate<PropertyMeta<?, ?>>()
	{
		@Override
		public boolean apply(PropertyMeta<?, ?> pm)
		{
			return pm.type().isJoinColumn() && !pm.type().isProxyType();
		}
	};

	private Predicate<PropertyMeta<?, ?>> proxyFilter = new Predicate<PropertyMeta<?, ?>>()
	{
		@Override
		public boolean apply(PropertyMeta<?, ?> pm)
		{
			return pm.type().isProxyType();
		}
	};

	public void persist(CQLEntityPersister entityPersister, CQLPersistenceContext context)
	{
		context.bindForInsert();
		cascadePersist(entityPersister, context);
	}

	public boolean doesEntityExist(CQLPersistenceContext context)
	{
		return context.checkForEntityExistence();
	}

	public void remove(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		context.bindForRemoval(entityMeta.getTableName(), entityMeta.getWriteConsistencyLevel());
		removeLinkedTables(context);
	}

	protected void cascadePersist(CQLEntityPersister entityPersister, CQLPersistenceContext context)
	{
		Object entity = context.getEntity();
		List<PropertyMeta<?, ?>> allMetas = context.getEntityMeta().getAllMetas();

		Collection<PropertyMeta<?, ?>> joinSimples = filter(allMetas, joinFilter);
		for (PropertyMeta<?, ?> pm : joinSimples)
		{
			Object joinValue = invoker.getValueFromField(entity, pm.getGetter());
			if (joinValue != null)
			{
				if (joinValue instanceof List || joinValue instanceof Set)
				{
					doCascadeCollection(entityPersister, context, pm, (Collection<?>) joinValue);
				}
				else if (joinValue instanceof Map)
				{
					Map<?, ?> joinMap = (Map<?, ?>) joinValue;
					doCascadeCollection(entityPersister, context, pm, joinMap.values());
				}
				else
				{
					doCascade(entityPersister, context, pm, joinValue);
				}
			}
		}
	}

	protected void removeLinkedTables(CQLPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();

		List<PropertyMeta<?, ?>> allMetas = entityMeta.getAllMetas();
		Collection<PropertyMeta<?, ?>> proxyMetas = filter(allMetas, proxyFilter);
		for (PropertyMeta<?, ?> pm : proxyMetas)
		{
			context.bindForRemoval(pm.getExternalTableName(), pm.getWriteConsistencyLevel());

		}
	}

	private void doCascadeCollection(CQLEntityPersister entityPersister,
			CQLPersistenceContext context, PropertyMeta<?, ?> pm, Collection<?> joinCollection)
	{
		for (Object joinEntity : joinCollection)
		{
			doCascade(entityPersister, context, pm, joinEntity);
		}
	}

	private void doCascade(CQLEntityPersister entityPersister, CQLPersistenceContext context,
			PropertyMeta<?, ?> pm, Object joinEntity)
	{
		CQLPersistenceContext joinContext = (CQLPersistenceContext) context.newPersistenceContext(
				pm.getJoinProperties().getEntityMeta(), joinEntity);
		entityPersister.cascadePersistOrEnsureExist(joinContext, pm.getJoinProperties());
	}
}
