package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;

/**
 * AchillesEntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityPersister
{

	public void persist(AchillesPersistenceContext context);

	public void remove(AchillesPersistenceContext context);

}