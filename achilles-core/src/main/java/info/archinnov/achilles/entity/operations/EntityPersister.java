package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.PersistenceContext;

/**
 * EntityPersister
 * 
 * @author DuyHai DOAN
 * 
 */
public interface EntityPersister<CONTEXT extends PersistenceContext> {

    public void persist(CONTEXT context);

    public void remove(CONTEXT context);

}
