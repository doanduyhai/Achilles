package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.multiValuesNonProxyTypes;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.proxy.MethodInvoker;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftMergerImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftMergerImpl implements Merger<ThriftPersistenceContext>
{

	private static final Logger log = LoggerFactory.getLogger(ThriftMergerImpl.class);
	private ThriftEntityPersister persister = new ThriftEntityPersister();
	private MethodInvoker invoker = new MethodInvoker();

	@Override
	public void merge(ThriftPersistenceContext context, Map<Method, PropertyMeta<?, ?>> dirtyMap)
	{
		if (dirtyMap.size() > 0)
		{
			Object entity = context.getEntity();
			for (Entry<Method, PropertyMeta<?, ?>> entry : dirtyMap.entrySet())
			{
				PropertyMeta<?, ?> pm = entry.getValue();
				boolean removeProperty = invoker.getValueFromField(entity, pm.getGetter()) == null;

				if (removeProperty)
				{
					log.debug("Removing property {}", pm.getPropertyName());
					persister.removePropertyBatch(context, pm);
				}
				else
				{
					if (multiValuesNonProxyTypes.contains(pm.type()))
					{
						log.debug("Removing dirty collection/map {} before merging",
								pm.getPropertyName());
						persister.removePropertyBatch(context, pm);
					}
					persister.persistPropertyBatch(context, pm);
				}
			}
		}

		dirtyMap.clear();

	}

	@Override
	public void cascadeMerge(EntityMerger<ThriftPersistenceContext> entityMerger,
			ThriftPersistenceContext context, List<PropertyMeta<?, ?>> joinPMs)
	{

		Object entity = context.getEntity();
		for (PropertyMeta<?, ?> pm : joinPMs)
		{
			log.debug("Cascade-merging join property {}", pm.getPropertyName());
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

	private void doCascade(EntityMerger<ThriftPersistenceContext> entityMerger,
			ThriftPersistenceContext context, PropertyMeta<?, ?> pm, Object joinEntity)
	{

		if (joinEntity != null)
		{
			log.debug("Merging join entity {} ", joinEntity);
			ThriftPersistenceContext joinContext = context.newPersistenceContext(pm.joinMeta(),
					joinEntity);

			entityMerger.merge(joinContext, joinEntity);
		}
	}

	private void doCascadeCollection(EntityMerger<ThriftPersistenceContext> entityMerger,
			ThriftPersistenceContext context, PropertyMeta<?, ?> pm, Collection<?> joinEntities)
	{
		log.debug("Merging join collection of entity {} ", joinEntities);
		for (Object joinEntity : joinEntities)
		{
			doCascade(entityMerger, context, pm, joinEntity);
		}
	}

}
