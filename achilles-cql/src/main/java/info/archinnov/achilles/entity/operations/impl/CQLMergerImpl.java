package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityMerger.PropertyMetaComparator;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * CQLMergerImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLMergerImpl
{
	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();
	private PropertyMetaComparator comparator = new PropertyMetaComparator();

	public void merge(CQLPersistenceContext context, Map<Method, PropertyMeta<?, ?>> dirtyMap)
	{
		if (!dirtyMap.isEmpty())
		{
			List<PropertyMeta<?, ?>> sortedDirtyMetas = new ArrayList<PropertyMeta<?, ?>>(
					dirtyMap.values());
			Collections.sort(sortedDirtyMetas, comparator);
			context.bindForUpdate(sortedDirtyMetas);
			dirtyMap.clear();
		}
	}

	public void cascadeMerge(CQLEntityMerger entityMerger, CQLPersistenceContext context,
			List<PropertyMeta<?, ?>> joinPMs)
	{
		Object entity = context.getEntity();
		for (PropertyMeta<?, ?> pm : joinPMs)
		{
			Object joinValue = invoker.getValueFromField(entity, pm.getGetter());
			if (joinValue != null)
			{
				if (pm.isJoinCollection())
				{
					doCascadeCollection(entityMerger, context, pm, (Collection<?>) joinValue);
				}
				else if (pm.isJoinMap())
				{
					Map<?, ?> joinMap = (Map<?, ?>) joinValue;
					doCascadeCollection(entityMerger, context, pm, joinMap.values());
				}
				else
				{
					doCascade(entityMerger, context, pm, joinValue);
				}
			}
		}
	}

	private void doCascadeCollection(CQLEntityMerger entityMerger, CQLPersistenceContext context,
			PropertyMeta<?, ?> pm, Collection<?> joinCollection)
	{
		for (Object joinEntity : joinCollection)
		{
			doCascade(entityMerger, context, pm, joinEntity);
		}
	}

	private void doCascade(CQLEntityMerger entityMerger, CQLPersistenceContext context,
			PropertyMeta<?, ?> pm, Object joinEntity)
	{
		CQLPersistenceContext joinContext = context.newPersistenceContext(pm
				.getJoinProperties()
				.getEntityMeta(), joinEntity);
		entityMerger.merge(joinContext, joinEntity);
	}
}
