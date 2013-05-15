package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.interceptor.JpaEntityInterceptor;
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
 * EntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityMerger implements AchillesEntityMerger
{
	private static final Logger log = LoggerFactory.getLogger(ThriftEntityMerger.class);

	private ThriftEntityPersister persister = new ThriftEntityPersister();
	private AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();
	private AchillesEntityProxifier proxifier = new AchillesEntityProxifier();
	private Set<PropertyType> multiValueTypes = Sets.newHashSet(LIST, LAZY_LIST, SET, LAZY_SET,
			MAP, LAZY_MAP);

	@Override
	@SuppressWarnings("unchecked")
	public <T, ID> T mergeEntity(AchillesPersistenceContext<ID> context)
	{
		log.debug("Merging entity of class {} with primary key {}", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());

		ThriftPersistenceContext<ID> thriftContext = (ThriftPersistenceContext<ID>) context;
		T entity = (T) context.getEntity();
		EntityMeta<ID> entityMeta = context.getEntityMeta();

		Validator.validateNotNull(entity, "Proxy object should not be null");
		Validator.validateNotNull(entityMeta, "entityMeta should not be null");

		T proxy;
		if (proxifier.isProxy(entity))
		{
			log.debug("Checking for dirty fields before merging");

			T realObject = proxifier.getRealObject(entity);
			JpaEntityInterceptor<ID, T> interceptor = (JpaEntityInterceptor<ID, T>) proxifier
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
						persister.removePropertyBatch(thriftContext, propertyMeta);
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
					Set<CascadeType> cascadeTypes = propertyMeta.getJoinProperties()
							.getCascadeTypes();
					if (cascadeTypes.contains(MERGE) || cascadeTypes.contains(ALL))
					{
						log.debug("Cascade-merging join property {}",
								propertyMeta.getPropertyName());

						switch (propertyMeta.type())
						{
							case JOIN_SIMPLE:
								mergeJoinProperty(thriftContext, realObject, propertyMeta);
								break;
							case JOIN_LIST:
								mergeJoinListProperty(thriftContext, realObject, propertyMeta);
								break;
							case JOIN_SET:
								mergeJoinSetProperty(thriftContext, realObject, propertyMeta);
								break;
							case JOIN_MAP:
								mergeJoinMapProperty(thriftContext, realObject, propertyMeta);
								break;
							default:
								break;
						}
					}
				}
			}
			interceptor.setContext(thriftContext);
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

	private <T, ID> void mergeJoinProperty(ThriftPersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{

		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Object joinEntity = introspector.getValueFromField(entity, propertyMeta.getGetter());

		if (joinEntity != null)
		{
			log.debug("Merging join entity {} ", joinEntity);
			Object mergedEntity = mergeEntity(context.newPersistenceContext(
					joinProperties.getEntityMeta(), joinEntity));
			introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntity);
		}
	}

	private <T, ID> void mergeJoinListProperty(ThriftPersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		List<?> joinEntities = (List<?>) introspector.getValueFromField(entity,
				propertyMeta.getGetter());
		List<Object> mergedEntities = new ArrayList<Object>();
		mergeCollectionOfJoinEntities(context, joinProperties, joinEntities, mergedEntities);
		introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntities);
	}

	private <T, ID> void mergeJoinSetProperty(ThriftPersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Set<?> joinEntities = (Set<?>) introspector.getValueFromField(entity,
				propertyMeta.getGetter());
		Set<Object> mergedEntities = new HashSet<Object>();
		mergeCollectionOfJoinEntities(context, joinProperties, joinEntities, mergedEntities);
		introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntities);
	}

	private <ID> void mergeCollectionOfJoinEntities(ThriftPersistenceContext<ID> context,
			JoinProperties joinProperties, Collection<?> joinEntities,
			Collection<Object> mergedEntities)
	{
		if (joinEntities != null)
		{
			log.debug("Merging join collection of entity {} ", joinEntities);
			for (Object joinEntity : joinEntities)
			{
				Object mergedEntity = mergeEntity(context.newPersistenceContext(
						joinProperties.getEntityMeta(), joinEntity));
				mergedEntities.add(mergedEntity);
			}
		}
	}

	private <T, ID> void mergeJoinMapProperty(ThriftPersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Map<?, ?> joinEntitiesMap = (Map<?, ?>) introspector.getValueFromField(entity,
				propertyMeta.getGetter());
		Map<Object, Object> mergedEntitiesMap = new HashMap<Object, Object>();
		if (joinEntitiesMap != null)
		{
			log.debug("Merging join map of entity {} ", joinEntitiesMap);
			for (Entry<?, ?> joinEntityEntry : joinEntitiesMap.entrySet())
			{
				Object mergedEntity = this.mergeEntity(context.newPersistenceContext(
						joinProperties.getEntityMeta(), joinEntityEntry.getValue()));
				mergedEntitiesMap.put(joinEntityEntry.getKey(), mergedEntity);
			}
		}
		introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntitiesMap);
	}

	public void setPersister(ThriftEntityPersister persister)
	{
		this.persister = persister;
	}
}
