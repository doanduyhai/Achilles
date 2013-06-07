package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import java.util.Map;

/**
 * CqlEntityManager
 * 
 * @author DuyHai DOAN
 * 
 */
public class CqlEntityManager extends AchillesEntityManager<CQLPersistenceContext> {
    private CQLDaoContext daoContext;

    protected CqlEntityManager(AchillesEntityManagerFactory entityManagerFactory,
            Map<Class<?>, EntityMeta> entityMetaMap, //
            ConfigurationContext configContext, CQLDaoContext daoContext) {
        super(entityManagerFactory, entityMetaMap, configContext);
        this.daoContext = daoContext;
    }

    @Override
    public void persist(Object entity, ConsistencyLevel writeLevel) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#merge(java.lang.Object,
     * info.archinnov.achilles.type.ConsistencyLevel)
     */
    @Override
    public <T> T merge(T entity, ConsistencyLevel writeLevel) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#remove(java.lang.Object,
     * info.archinnov.achilles.type.ConsistencyLevel)
     */
    @Override
    public void remove(Object entity, ConsistencyLevel writeLevel) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#find(java.lang.Class, java.lang.Object,
     * info.archinnov.achilles.type.ConsistencyLevel)
     */
    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, ConsistencyLevel readLevel) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#getReference(java.lang.Class,
     * java.lang.Object, info.archinnov.achilles.type.ConsistencyLevel)
     */
    @Override
    public <T> T getReference(Class<T> entityClass, Object primaryKey, ConsistencyLevel readLevel) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#refresh(java.lang.Object,
     * info.archinnov.achilles.type.ConsistencyLevel)
     */
    @Override
    public void refresh(Object entity, ConsistencyLevel readLevel) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#initPersistenceContext(java.lang.Object)
     */
    @Override
    protected CQLPersistenceContext initPersistenceContext(Object entity) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see info.archinnov.achilles.entity.manager.AchillesEntityManager#initPersistenceContext(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    protected CQLPersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey) {
        // TODO Auto-generated method stub
        return null;
    }

}
