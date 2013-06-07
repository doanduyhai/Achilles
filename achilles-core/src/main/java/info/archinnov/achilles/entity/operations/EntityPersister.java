package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;

/**
 * EntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public interface EntityPersister<CONTEXT extends AchillesPersistenceContext> {

    public void persist(CONTEXT context);

    public void remove(CONTEXT context);

}
