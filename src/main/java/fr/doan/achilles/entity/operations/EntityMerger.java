package fr.doan.achilles.entity.operations;

import static fr.doan.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.MERGE;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.CascadeType;

import net.sf.cglib.proxy.Factory;
import fr.doan.achilles.dao.GenericDynamicCompositeDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;
import fr.doan.achilles.proxy.interceptor.JpaEntityInterceptor;
import fr.doan.achilles.validation.Validator;

/**
 * EntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityMerger
{
	private EntityPersister persister = new EntityPersister();

	private EntityProxyBuilder interceptorBuilder = new EntityProxyBuilder();

	private EntityHelper helper = new EntityHelper();

	@SuppressWarnings("unchecked")
	public <T, ID> T mergeEntity(T entity, EntityMeta<ID> entityMeta)
	{
		Validator.validateNotNull(entity, "proxy object");
		Validator.validateNotNull(entityMeta, "entityMeta");

		T proxy;
		if (helper.isProxy(entity))
		{
			Factory factory = (Factory) entity;
			JpaEntityInterceptor<ID> interceptor = (JpaEntityInterceptor<ID>) factory
					.getCallback(0);
			GenericDynamicCompositeDao<ID> dao = entityMeta.getEntityDao();

			Map<Method, PropertyMeta<?, ?>> dirtyMap = interceptor.getDirtyMap();

			Object realObject = interceptor.getTarget();
			for (Entry<Method, PropertyMeta<?, ?>> entry : dirtyMap.entrySet())
			{
				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				ID key = interceptor.getKey();
				if (propertyMeta.type().isMultiValue())
				{
					this.persister.removeProperty(key, dao, propertyMeta);
				}
				this.persister.persistProperty(realObject, key, dao, propertyMeta);
			}
			dirtyMap.clear();

			for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
			{

				PropertyMeta<?, ?> propertyMeta = entry.getValue();
				if (propertyMeta.type() == JOIN_SIMPLE)
				{

					JoinProperties joinProperties = propertyMeta.getJoinProperties();
					List<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
					if (cascadeTypes.contains(MERGE) || cascadeTypes.contains(ALL))
					{
						Object joinEntity = helper.getValueFromField(entity,
								propertyMeta.getGetter());
						if (joinEntity != null)
						{
							Object mergedEntity = this.mergeEntity(joinEntity,
									joinProperties.getEntityMeta());
							helper.setValueToField(entity, propertyMeta.getSetter(), mergedEntity);
						}
					}
				}
			}
			proxy = entity;
		}
		else
		{
			this.persister.persist(entity, entityMeta);
			proxy = (T) interceptorBuilder.build(entity, entityMeta);
		}

		return proxy;
	}
}
