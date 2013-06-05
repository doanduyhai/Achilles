package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;

/**
 * AchillesEntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityMerger
{
	public <T> T merge(AchillesPersistenceContext context, T entity);

}