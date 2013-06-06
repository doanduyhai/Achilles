package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.AchillesEntityInterceptor;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.persistence.CascadeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * ThriftEntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityMerger implements AchillesEntityMerger<ThriftPersistenceContext>
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityMerger.class);

	private ThriftEntityPersister persister = new ThriftEntityPersister();
	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();
	private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
	private Set<PropertyType> multiValueTypes = Sets.newHashSet(LIST, LAZY_LIST, SET, LAZY_SET,
			MAP, LAZY_MAP);

	@Override
	public <T> T merge(ThriftPersistenceContext context, T entity)
	{
		log.debug("Merging entity of class {} with primary key {}", context
				.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		EntityMeta entityMeta = context.getEntityMeta();

		Validator.validateNotNull(entity, "Proxy object should not be null");
		Validator.validateNotNull(entityMeta, "entityMeta should not be null");

		T proxy;
		if (proxifier.isProxy(entity))
		{
			log.debug("Checking for dirty fields before merging");

			T realObject = proxifier.getRealObject(entity);
			AchillesEntityInterceptor<ThriftPersistenceContext, T> interceptor = proxifier
					.getInterceptor(entity);
			Map<Method, PropertyMeta<?, ?>> dirtyMap = interceptor.getDirtyMap();

			if (dirtyMap.size() > 0)
			{
				for (Entry<Method, PropertyMeta<?, ?>> entry : dirtyMap.entrySet())
				{
					PropertyMeta<?, ?> propertyMeta = entry.getValue();
					if (multiValueTypes.contains(propertyMeta.type()))
					{
						log.debug("Removing dirty collection/map {} before merging",
								propertyMeta.getPropertyName());
						persister.removePropertyBatch(context, propertyMeta);
					}
					persister.persistProperty(context, propertyMeta);
				}
			}

			dirtyMap.clear();

			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				if (propertyMeta.isJoin())
				{
					Set<CascadeType> cascadeTypes = propertyMeta
							.getJoinProperties()
							.getCascadeTypes();
					if (cascadeTypes.contains(MERGE) || cascadeTypes.contains(ALL))
					{
						log.debug("Cascade-merging join property {}",
								propertyMeta.getPropertyName());

						switch (propertyMeta.type())
						{
							case JOIN_SIMPLE:
								mergeJoinProperty(context, realObject, propertyMeta);
								break;
							case JOIN_LIST:
								mergeJoinListProperty(context, realObject, propertyMeta);
								break;
							case JOIN_SET:
								mergeJoinSetProperty(context, realObject, propertyMeta);
								break;
							case JOIN_MAP:
								mergeJoinMapProperty(context, realObject, propertyMeta);
								break;
							default:
								break;
						}
					}
				}
			}
			interceptor.setContext(context);
			interceptor.setTarget(realObject);
			proxy = entity;
		}
		else
		{
			log.debug("Persisting transient entity");

			if (!context.isWideRow())
			{
				this.persister.persist(context);
			}

			proxy = proxifier.buildProxy(entity, context);
		}

		return proxy;
	}

	private <T> void mergeJoinProperty(ThriftPersistenceContext context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{

		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Object joinEntity = invoker.getValueFromField(entity, propertyMeta.getGetter());

		if (joinEntity != null)
		{
			log.debug("Merging join entity {} ", joinEntity);
			Object mergedEntity = merge(
					context.newPersistenceContext(joinProperties.getEntityMeta(), joinEntity),
					joinEntity);
			invoker.setValueToField(entity, propertyMeta.getSetter(), mergedEntity);
		}
	}

	private void mergeJoinListProperty(ThriftPersistenceContext context, Object entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		List<?> joinEntities = (List<?>) invoker
				.getValueFromField(entity, propertyMeta.getGetter());
		List<Object> mergedEntities = new ArrayList<Object>();
		mergeCollectionOfJoinEntities(context, joinProperties, joinEntities, mergedEntities);
		invoker.setValueToField(entity, propertyMeta.getSetter(), mergedEntities);
	}

	private void mergeJoinSetProperty(ThriftPersistenceContext context, Object entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Set<?> joinEntities = (Set<?>) invoker.getValueFromField(entity, propertyMeta.getGetter());
		Set<Object> mergedEntities = new HashSet<Object>();
		mergeCollectionOfJoinEntities(context, joinProperties, joinEntities, mergedEntities);
		invoker.setValueToField(entity, propertyMeta.getSetter(), mergedEntities);
	}

	private void mergeCollectionOfJoinEntities(ThriftPersistenceContext context,
			JoinProperties joinProperties, Collection<?> joinEntities,
			Collection<Object> mergedEntities)
	{
		if (joinEntities != null)
		{
			log.debug("Merging join collection of entity {} ", joinEntities);
			for (Object joinEntity : joinEntities)
			{
				Object mergedEntity = merge(
						context.newPersistenceContext(joinProperties.getEntityMeta(), joinEntity),
						joinEntity);
				mergedEntities.add(mergedEntity);
			}
		}
	}

	private void mergeJoinMapProperty(ThriftPersistenceContext context, Object entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Map<?, ?> joinEntitiesMap = (Map<?, ?>) invoker.getValueFromField(entity,
				propertyMeta.getGetter());
		Map<Object, Object> mergedEntitiesMap = new HashMap<Object, Object>();
		if (joinEntitiesMap != null)
		{
			log.debug("Merging join map of entity {} ", joinEntitiesMap);
			for (Entry<?, ?> joinEntityEntry : joinEntitiesMap.entrySet())
			{
				Object mergedEntity = this.merge(context.newPersistenceContext(
						joinProperties.getEntityMeta(), joinEntityEntry.getValue()),
						joinEntityEntry.getValue());
				mergedEntitiesMap.put(joinEntityEntry.getKey(), mergedEntity);
			}
		}
		invoker.setValueToField(entity, propertyMeta.getSetter(), mergedEntitiesMap);
	}

	public void setPersister(ThriftEntityPersister persister)
	{
		this.persister = persister;
	}
}
