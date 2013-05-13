package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.entity.context.AchillesPersistenceContext;

/**
 * AchillesEntityLoader
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesEntityLoader
{

	public <T, ID> T load(AchillesPersistenceContext<ID> context);

}