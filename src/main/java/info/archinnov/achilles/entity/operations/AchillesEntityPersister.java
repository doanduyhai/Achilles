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

	public <ID> void persist(AchillesPersistenceContext<ID> context);

	public <ID> void remove(AchillesPersistenceContext<ID> context);

}