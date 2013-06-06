package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;

/**
 * AchillesEntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityMerger<CONTEXT extends AchillesPersistenceContext>
{
	public <T> T merge(CONTEXT context, T entity);

}