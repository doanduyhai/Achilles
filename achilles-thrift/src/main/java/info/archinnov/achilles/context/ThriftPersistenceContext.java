package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.entity.operations.EntityValidator;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityMerger;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import me.prettyprint.hector.api.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPersistenceContext
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPersistenceContext extends AchillesPersistenceContext {
    private static final Logger log = LoggerFactory.getLogger(ThriftPersistenceContext.class);

    private ThriftEntityPersister persister = new ThriftEntityPersister();
    private ThriftEntityLoader loader = new ThriftEntityLoader();
    private ThriftEntityMerger merger = new ThriftEntityMerger();
    private ThriftEntityProxifier proxifier = new ThriftEntityProxifier();
    private EntityValidator<ThriftPersistenceContext> entityValidator;
    private EntityRefresher<ThriftPersistenceContext> refresher;

    private ThriftDaoContext thriftDaoContext;
    private ThriftGenericEntityDao entityDao;
    private ThriftGenericWideRowDao wideRowDao;
    private ThriftAbstractFlushContext thriftFlushContext;
    private AchillesConsistencyLevelPolicy policy;

    public ThriftPersistenceContext(EntityMeta entityMeta, //
            ConfigurationContext configContext, //
            ThriftDaoContext thriftDaoContext, //
            ThriftAbstractFlushContext flushContext, //
            Object entity) {
        super(entityMeta, configContext, entity, flushContext);
        log.trace("Create new persistence context for instance {} of class {}", entity, entityMeta.getClassName());

        initCollaborators(thriftDaoContext, flushContext);
        initDaos();
    }

    public ThriftPersistenceContext(EntityMeta entityMeta, //
            ConfigurationContext configContext, //
            ThriftDaoContext thriftDaoContext, //
            ThriftAbstractFlushContext flushContext, //
            Class<?> entityClass, Object primaryKey) {
        super(entityMeta, configContext, entityClass, primaryKey, flushContext);
        log.trace("Create new persistence context for instance {} of class {}", entity,
                entityClass.getCanonicalName());

        initCollaborators(thriftDaoContext, flushContext);
        initDaos();
    }

    private void initCollaborators(ThriftDaoContext thriftDaoContext, //
            ThriftAbstractFlushContext flushContext) {
        entityValidator = new EntityValidator<ThriftPersistenceContext>(proxifier);
        refresher = new EntityRefresher<ThriftPersistenceContext>(loader, proxifier);
        policy = configContext.getConsistencyPolicy();
        this.thriftDaoContext = thriftDaoContext;
        this.thriftFlushContext = flushContext;
    }

    private void initDaos() {
        String columnFamilyName = entityMeta.getTableName();
        if (entityMeta.isWideRow()) {
            this.wideRowDao = thriftDaoContext.findWideRowDao(columnFamilyName);
        } else {
            this.entityDao = thriftDaoContext.findEntityDao(columnFamilyName);
        }
    }

    @Override
    public ThriftPersistenceContext newPersistenceContext(EntityMeta joinMeta, Object joinEntity) {
        log.trace("Spawn new persistence context for instance {} of join class {}", joinEntity,
                joinMeta.getClassName());
        return new ThriftPersistenceContext(joinMeta, configContext, thriftDaoContext, thriftFlushContext, joinEntity);
    }

    @Override
    public ThriftPersistenceContext newPersistenceContext(Class<?> entityClass, EntityMeta joinMeta, Object joinId) {
        log.trace("Spawn new persistence context for primary key {} of join class {}", joinId,
                joinMeta.getClassName());

        return new ThriftPersistenceContext(joinMeta, configContext, thriftDaoContext, thriftFlushContext,
                entityClass, joinId);
    }

    @Override
    public void persist(Object entity) {
        // TODO Auto-generated method stub

    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T merge(T entity) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void remove(Object entity) {
        // TODO Auto-generated method stub

    }

    public ThriftGenericEntityDao findEntityDao(String columnFamilyName) {
        return thriftDaoContext.findEntityDao(columnFamilyName);
    }

    public ThriftGenericWideRowDao findWideRowDao(String columnFamilyName) {
        return thriftDaoContext.findWideRowDao(columnFamilyName);
    }

    public ThriftCounterDao getCounterDao() {
        return thriftDaoContext.getCounterDao();
    }

    public Mutator<Object> getCurrentColumnFamilyMutator() {
        return thriftFlushContext.getWideRowMutator(entityMeta.getTableName());
    }

    public Mutator<Object> getWideRowMutator(String columnFamilyName) {
        return thriftFlushContext.getWideRowMutator(columnFamilyName);
    }

    public Mutator<Object> getEntityMutator(String columnFamilyName) {
        return thriftFlushContext.getEntityMutator(columnFamilyName);
    }

    public Mutator<Object> getCounterMutator() {
        return thriftFlushContext.getCounterMutator();
    }

    public ThriftGenericEntityDao getEntityDao() {
        return entityDao;
    }

    public ThriftGenericWideRowDao getColumnFamilyDao() {
        return wideRowDao;
    }

    public AchillesConsistencyLevelPolicy getPolicy() {
        return policy;
    }
}
