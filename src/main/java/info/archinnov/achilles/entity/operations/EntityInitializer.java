package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityInitializer
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntityInitializer
{
	private static final Logger log = LoggerFactory.getLogger(EntityInitializer.class);

	public <T> void initializeEntity(T entity, EntityMeta<?> entityMeta)
	{
		for (PropertyMeta<?, ?> propertyMeta : entityMeta.getPropertyMetas().values())
		{
			PropertyType type = propertyMeta.type();
			if (type.isLazy() && !type.isWideMap())
			{
				try
				{
					propertyMeta.getGetter().invoke(entity);
				}
				catch (Exception e)
				{
					log.error("Cannot initialize property '" + propertyMeta.getPropertyName()
							+ "' for entity '" + entity + "'", e);
				}
			}
		}
	}
}
