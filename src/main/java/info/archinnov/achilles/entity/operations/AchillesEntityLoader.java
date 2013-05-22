package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

/**
 * AchillesEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityLoader
{

	public <T> T load(AchillesPersistenceContext context, Class<T> entityClass);

	public <V> void loadPropertyIntoObject(Object realObject, Object key,
			AchillesPersistenceContext context, PropertyMeta<?, V> propertyMeta);
}