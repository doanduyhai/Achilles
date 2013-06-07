package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;

/**
 * EntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public interface EntityMerger<CONTEXT extends AchillesPersistenceContext> {
    public <T> T merge(CONTEXT context, T entity);

}
