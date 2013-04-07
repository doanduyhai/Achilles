package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.MERGE;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.PersistenceContext;
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

import com.google.common.collect.Sets;

/**
 * EntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMerger
{
	private EntityPersister persister = new EntityPersister();
	private EntityIntrospector introspector = new EntityIntrospector();
	private EntityProxifier proxifier = new EntityProxifier();
	private Set<PropertyType> multiValueTypes = Sets.newHashSet(LIST, LAZY_LIST, SET, LAZY_SET,
			MAP, LAZY_MAP);

	@SuppressWarnings("unchecked")
	public <T, ID> T mergeEntity(PersistenceContext<ID> context)
	{
		T entity = (T) context.getEntity();
		EntityMeta<ID> entityMeta = context.getEntityMeta();

		Validator.validateNotNull(entity, "Proxy object should not be null");
		Validator.validateNotNull(entityMeta, "entityMeta should not be null");

		T proxy;
		if (proxifier.isProxy(entity))
		{
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
						this.persister.removePropertyBatch(context, propertyMeta);
					}
					this.persister.persistProperty(context, propertyMeta);
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
			interceptor.setTarget(realObject);
			proxy = entity;
		}
		else
		{
			if (!context.isDirectColumnFamilyMapping())
			{
				this.persister.persist(context);
			}

			proxy = proxifier.buildProxy(entity, context);
		}

		return proxy;
	}

	private <T, ID> void mergeJoinProperty(PersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Object joinEntity = introspector.getValueFromField(entity, propertyMeta.getGetter());
		if (joinEntity != null)
		{
			Object mergedEntity = mergeEntity(context.newPersistenceContext(
					joinProperties.getEntityMeta(), joinEntity));
			introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntity);
		}
	}

	private <T, ID> void mergeJoinListProperty(PersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		List<?> joinEntities = (List<?>) introspector.getValueFromField(entity,
				propertyMeta.getGetter());
		List<Object> mergedEntities = new ArrayList<Object>();
		mergeCollectionOfJoinEntities(context, joinProperties, joinEntities, mergedEntities);
		introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntities);
	}

	private <T, ID> void mergeJoinSetProperty(PersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Set<?> joinEntities = (Set<?>) introspector.getValueFromField(entity,
				propertyMeta.getGetter());
		Set<Object> mergedEntities = new HashSet<Object>();
		mergeCollectionOfJoinEntities(context, joinProperties, joinEntities, mergedEntities);
		introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntities);
	}

	private <ID> void mergeCollectionOfJoinEntities(PersistenceContext<ID> context,
			JoinProperties joinProperties, Collection<?> joinEntities,
			Collection<Object> mergedEntities)
	{
		if (joinEntities != null)
		{
			for (Object joinEntity : joinEntities)
			{
				Object mergedEntity = mergeEntity(context.newPersistenceContext(
						joinProperties.getEntityMeta(), joinEntity));
				mergedEntities.add(mergedEntity);
			}
		}
	}

	private <T, ID> void mergeJoinMapProperty(PersistenceContext<ID> context, T entity,
			PropertyMeta<?, ?> propertyMeta)
	{
		JoinProperties joinProperties = propertyMeta.getJoinProperties();
		Map<?, ?> joinEntitiesMap = (Map<?, ?>) introspector.getValueFromField(entity,
				propertyMeta.getGetter());
		Map<Object, Object> mergedEntitiesMap = new HashMap<Object, Object>();
		if (joinEntitiesMap != null)
		{
			for (Entry<?, ?> joinEntityEntry : joinEntitiesMap.entrySet())
			{
				Object mergedEntity = this.mergeEntity(context.newPersistenceContext(
						joinProperties.getEntityMeta(), joinEntityEntry.getValue()));
				mergedEntitiesMap.put(joinEntityEntry.getKey(), mergedEntity);
			}
		}
		introspector.setValueToField(entity, propertyMeta.getSetter(), mergedEntitiesMap);
	}

	public void setPersister(EntityPersister persister)
	{
		this.persister = persister;
	}
}
