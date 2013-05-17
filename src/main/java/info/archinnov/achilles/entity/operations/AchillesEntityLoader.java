package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

/**
 * AchillesEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityLoader
{

	public <T, ID> T load(AchillesPersistenceContext<ID> context);

	public <ID, V> void loadPropertyIntoObject(Object realObject, ID key,
			AchillesPersistenceContext<ID> context, PropertyMeta<?, V> propertyMeta);
}