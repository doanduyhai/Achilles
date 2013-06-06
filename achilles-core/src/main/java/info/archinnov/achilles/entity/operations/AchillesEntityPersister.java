package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;

/**
 * AchillesEntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityPersister<CONTEXT extends AchillesPersistenceContext>
{

	public void persist(CONTEXT context);

	public void remove(CONTEXT context);

}