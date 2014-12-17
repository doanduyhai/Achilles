/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.context;

import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.interceptor.Event.POST_LOAD;
import static info.archinnov.achilles.interceptor.Event.POST_INSERT;
import static info.archinnov.achilles.interceptor.Event.POST_DELETE;
import static info.archinnov.achilles.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.interceptor.Event.PRE_INSERT;
import static info.archinnov.achilles.interceptor.Event.PRE_DELETE;
import static info.archinnov.achilles.interceptor.Event.PRE_UPDATE;
import static info.archinnov.achilles.type.Options.LWTCondition;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.type.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.async.AsyncUtils;
import info.archinnov.achilles.internal.async.ImmediateValue;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.facade.DaoOperations;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.internal.context.facade.PersistentStateHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityInitializer;
import info.archinnov.achilles.internal.persistence.operations.EntityLoader;
import info.archinnov.achilles.internal.persistence.operations.EntityPersister;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityRefresher;
import info.archinnov.achilles.internal.persistence.operations.EntityUpdater;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;

public class PersistenceContext {

    private static final Logger log = LoggerFactory.getLogger(PersistenceContext.class);

    protected AbstractFlushContext flushContext;
    protected EntityInitializer initializer = EntityInitializer.Singleton.INSTANCE.get();
    protected EntityPersister persister = EntityPersister.Singleton.INSTANCE.get();
    protected EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    protected EntityRefresher refresher = EntityRefresher.Singleton.INSTANCE.get();
    protected EntityLoader loader = EntityLoader.Singleton.INSTANCE.get();
    protected EntityUpdater updater = EntityUpdater.Singleton.INSTANCE.get();

    protected ConfigurationContext configContext;
    protected Class<?> entityClass;
    protected EntityMeta entityMeta;
    protected Object entity;
    protected Object primaryKey;
    protected Object partitionKey;

    protected Options options = OptionsBuilder.noOptions();

    protected DaoContext daoContext;

    private ConsistencyOverrider overrider = ConsistencyOverrider.Singleton.INSTANCE.get();
    protected AsyncUtils asyncUtils = AsyncUtils.Singleton.INSTANCE.get();

    protected PersistenceManagerFacade persistenceManagerFacade = new PersistenceManagerFacade();
    protected EntityFacade entityFacade = new EntityFacade();
    protected DaoFacade daoFacade = new DaoFacade();
    protected StateHolderFacade stateHolderFacade = new StateHolderFacade();

    private Function<PropertyMeta, Method> metaToGetter = new Function<PropertyMeta, Method>() {
        @Override
        public Method apply(PropertyMeta meta) {
            return meta.getGetter();
        }
    };

    public PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, DaoContext daoContext,
            AbstractFlushContext flushContext, Class<?> entityClass, Object primaryKey, Options options) {
        Validator.validateNotNull(entityClass, "The entity class should not be null for persistence context creation");
        Validator.validateNotNull(primaryKey,
                "The primary key for the entity class '{}' should not be null for persistence context creation",
                entityClass.getCanonicalName());
        this.entityMeta = entityMeta;
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.flushContext = flushContext;
        this.entityClass = entityClass;
        this.primaryKey = primaryKey;
        this.options = overrider.overrideRuntimeValueByBatchSetting(options, flushContext);
    }

    public PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, DaoContext daoContext,
            AbstractFlushContext flushContext, Object entity, Options options) {
        Validator.validateNotNull(entity,
                "The entity of type '{}' should not be null for persistence context creation",
                entityMeta.getClassName());
        this.primaryKey = entityMeta.forOperations().getPrimaryKey(entity);
        Validator.validateNotNull(primaryKey,
                "The primary key for the entity class '{}' should not be null for persistence context creation",
                entityMeta.getClassName());

        this.entityClass = entityMeta.getEntityClass();
        this.entityMeta = entityMeta;
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.flushContext = flushContext;
        this.entity = entity;
        this.options = overrider.overrideRuntimeValueByBatchSetting(options, flushContext);
    }

    public PersistenceContext duplicate(Object entity) {
        log.trace("Duplicate PersistenceContext for entity '{}'", entity);

        return new PersistenceContext(entityMeta, configContext, daoContext, flushContext.duplicate(), entity,
                options.duplicateWithoutTtlAndTimestamp());
    }

    public StateHolderFacade getStateHolderFacade() {
        return stateHolderFacade;
    }

    public EntityFacade getEntityFacade() {
        return entityFacade;
    }

    public PersistenceManagerFacade getPersistenceManagerFacade() {
        return persistenceManagerFacade;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(PersistenceContext.class).add("entity class", this.entityClass)
                .add("primary key", this.primaryKey).add("partition key", this.partitionKey).toString();
    }

    public class StateHolderFacade implements PersistentStateHolder {

        private StateHolderFacade() {
        }

        public PropertyMeta getIdMeta() {
            return entityMeta.getIdMeta();
        }

        public boolean isClusteredCounter() {
            return entityMeta.structure().isClusteredCounter();
        }

        public EntityMeta getEntityMeta() {
            return entityMeta;
        }

        public Object getEntity() {
            return entity;
        }

        public void setEntity(Object entity) {
            PersistenceContext.this.entity = entity;
        }

        @SuppressWarnings("unchecked")
        public <T> Class<T> getEntityClass() {
            return (Class<T>) entityClass;
        }

        public Object getPrimaryKey() {
            return primaryKey;
        }

        public Options getOptions() {
            return options;
        }

        public Optional<Integer> getTtl() {
            return options.getTtl();
        }

        public Optional<Long> getTimestamp() {
            return options.getTimestamp();
        }

        public Optional<ConsistencyLevel> getConsistencyLevel() {
            return options.getConsistencyLevel();
        }

        @Override
        public Optional<com.datastax.driver.core.ConsistencyLevel> getSerialConsistencyLevel() {
            return options.getSerialConsistency();
        }

        public List<LWTCondition> getLWTConditions() {
            return Optional.fromNullable(options.getLwtConditions()).or(new ArrayList<LWTCondition>());
        }

        public boolean hasLWTConditions() {
            return options.hasLWTConditions();
        }

        public Optional<LWTResultListener> getLWTResultListener() {
            return options.getLWTResultListener();
        }

        public Set<Method> getAllGettersExceptCounters() {
            return new HashSet<>(from(entityMeta.getAllMetasExceptCounters()).transform(metaToGetter).toList());
        }

        public List<PropertyMeta> getAllCountersMeta() {
            return entityMeta.getAllCounterMetas();
        }

        public ConfigurationContext getConfigContext() {
            return configContext;
        }

        public ExecutorService getExecutorService() {
            return configContext.getExecutorService();
        }
    }

    public class PersistenceManagerFacade extends StateHolderFacade implements PersistenceManagerOperations {

        private PersistenceManagerFacade() {
        }

        public <T> AchillesFuture<T> persist(final T rawEntity) {
            flushContext.triggerInterceptor(entityMeta, rawEntity, PRE_INSERT);
            persister.persist(entityFacade);
            final ListenableFuture<List<ResultSet>> resultSetFutures = flush();
            Function<List<ResultSet>, T> applyTriggers = new Function<List<ResultSet>, T>() {
                @Override
                public T apply(List<ResultSet> input) {
                    flushContext.triggerInterceptor(entityMeta, rawEntity, POST_INSERT);
                    return rawEntity;
                }
            };

            Function<T, T> createProxy = new Function<T, T>() {
                @Override
                public T apply(T input) {
                    return proxifier.buildProxyWithAllFieldsLoadedExceptCounters(rawEntity, entityFacade);
                }
            };

            final ListenableFuture<T> triggersApplied = asyncUtils.transformFuture(resultSetFutures, applyTriggers, getExecutorService());

            asyncUtils.maybeAddAsyncListeners(triggersApplied, options);

            final ListenableFuture<T> proxyCreated = asyncUtils.transformFuture(triggersApplied, createProxy);

            return asyncUtils.buildInterruptible(proxyCreated);
        }

        public <T> AchillesFuture<T> batchPersist(final T rawEntity) {
            flushContext.triggerInterceptor(entityMeta, rawEntity, PRE_INSERT);
            persister.persist(entityFacade);
            flush();
            flushContext.triggerInterceptor(entityMeta, rawEntity, POST_INSERT);
            final T proxy = proxifier.buildProxyWithAllFieldsLoadedExceptCounters(rawEntity, entityFacade);
            return asyncUtils.buildInterruptible(new ImmediateValue<>(proxy));
        }

        public <T> AchillesFuture<T> update(final T proxy) {
            flushContext.triggerInterceptor(entityMeta, proxy, PRE_UPDATE);
            updater.update(entityFacade, proxy);
            final ListenableFuture<List<ResultSet>> resultSetFutures = flush();
            Function<List<ResultSet>, T> applyTriggers = new Function<List<ResultSet>, T>() {
                @Override
                public T apply(List<ResultSet> input) {
                    flushContext.triggerInterceptor(entityMeta, proxy, POST_UPDATE);
                    return proxy;
                }
            };
            final ListenableFuture<T> triggersApplied = asyncUtils.transformFuture(resultSetFutures, applyTriggers, getExecutorService());
            asyncUtils.maybeAddAsyncListeners(triggersApplied, options);
            return asyncUtils.buildInterruptible(triggersApplied);
        }

        public <T> AchillesFuture<T> delete() {
            flushContext.triggerInterceptor(entityMeta, entity, PRE_DELETE);
            persister.delete(entityFacade);
            final ListenableFuture<List<ResultSet>> resultSetFutures = flush();
            Function<List<ResultSet>, T> applyTriggers = new Function<List<ResultSet>, T>() {
                @Override
                public T apply(List<ResultSet> input) {
                    flushContext.triggerInterceptor(entityMeta, entity, POST_DELETE);
                    return (T) entity;
                }
            };
            final ListenableFuture<T> triggersApplied = asyncUtils.transformFuture(resultSetFutures, applyTriggers, getExecutorService());
            asyncUtils.maybeAddAsyncListeners(triggersApplied, options);
            return asyncUtils.buildInterruptible(triggersApplied);
        }

        public AchillesFuture<Empty> deleteById() {
            persister.delete(entityFacade);
            final ListenableFuture<List<ResultSet>> resultSetFutures = flush();
            Function<List<ResultSet>, Empty> toEmpty = new Function<List<ResultSet>, Empty>() {
                @Override
                public Empty apply(List<ResultSet> input) {
                    return Empty.INSTANCE;
                }
            };
            final ListenableFuture<Empty> triggersApplied = asyncUtils.transformFuture(resultSetFutures, toEmpty, getExecutorService());
            asyncUtils.maybeAddAsyncListeners(triggersApplied, options);
            return asyncUtils.buildInterruptible(triggersApplied);
        }

        public <T> AchillesFuture<T> find(Class<T> entityClass) {
            final AchillesFuture<T> achillesFuture = loader.load(entityFacade, entityClass);

            Function<T, T> applyTrigger = new Function<T, T>() {
                @Override
                public T apply(T rawEntity) {
                    if (rawEntity != null) {
                        flushContext.triggerInterceptor(entityMeta, rawEntity, POST_LOAD);
                    }
                    return rawEntity;
                }
            };

            final ListenableFuture<T> triggersApplied = asyncUtils.transformFuture(achillesFuture, applyTrigger);

            asyncUtils.maybeAddAsyncListeners(triggersApplied, options);

            Function<T, T> createProxy = new Function<T, T>() {
                @Override
                public T apply(T rawEntity) {
                    return proxifier.buildProxyWithAllFieldsLoadedExceptCounters(rawEntity, entityFacade);
                }
            };

            final ListenableFuture<T> proxyCreated = asyncUtils.transformFuture(triggersApplied, createProxy);
            return asyncUtils.buildInterruptible(proxyCreated);
        }

        public <T> T getProxy(Class<T> entityClass) {
            T entity = loader.createEmptyEntity(entityFacade, entityClass);
            return proxifier.buildProxyWithNoFieldLoaded(entity, entityFacade);
        }

        public <T> T getProxyForUpdate(Class<T> entityClass) {
            T entity = loader.createEmptyEntity(entityFacade, entityClass);
            return proxifier.buildProxyForUpdate(entity, entityFacade);
        }

        public <T> AchillesFuture<T> refresh(T proxy) throws AchillesStaleObjectStateException {
            final AchillesFuture<T> achillesFuture = refresher.refresh(proxy, entityFacade);

            final Function<T, T> removeProxy = new Function<T, T>() {
                @Override
                public T apply(T proxy) {
                    return proxifier.removeProxy(proxy);
                }
            };

            final ListenableFuture<T> proxyRemoved = asyncUtils.transformFuture(achillesFuture, removeProxy);

            asyncUtils.maybeAddAsyncListeners(proxyRemoved, options);

            final Function<T, T> applyTriggers = new Function<T, T>() {
                @Override
                public T apply(T refreshedEntity) {
                    flushContext.triggerInterceptor(entityMeta, entity, POST_LOAD);
                    return refreshedEntity;
                }
            };

            final ListenableFuture<T> triggersApplied = asyncUtils.transformFuture(achillesFuture, applyTriggers);

            return asyncUtils.buildInterruptible(triggersApplied);
        }

        public <T> T initialize(T proxy) {
            initializer.initializeEntity(proxy, entityMeta);
            return proxy;
        }

        public <T> List<T> initialize(List<T> entities) {
            for (T entity : entities) {
                initialize(entity);
            }
            return entities;
        }

        public <T> Set<T> initialize(Set<T> entities) {
            for (T entity : entities) {
                initialize(entity);
            }
            return entities;
        }

        protected ListenableFuture<List<ResultSet>> flush() {
            return flushContext.flush();
        }

    }

    public class EntityFacade extends StateHolderFacade implements EntityOperations {

        private EntityFacade() {
        }

        public ListenableFuture<Row> loadEntity() {
            return daoContext.loadEntity(daoFacade);
        }

        public Row loadProperty(PropertyMeta pm) {
            return daoContext.loadProperty(daoFacade, pm);
        }

        public void pushInsertStatement() {
            final List<PropertyMeta> pms = entityMeta.forOperations().retrievePropertyMetasForInsert(entity);
            daoContext.pushInsertStatement(daoFacade, pms);
        }

        public void pushUpdateStatement(List<PropertyMeta> pms) {
            daoContext.pushUpdateStatement(daoFacade, pms);
        }

        public void pushCollectionAndMapUpdateStatements(DirtyCheckChangeSet changeSet) {
            daoContext.pushCollectionAndMapUpdateStatement(daoFacade, changeSet);
        }

        public void bindForDeletion() {
            daoContext.bindForDeletion(daoFacade, entityMeta);
        }

        // Simple counter
        public void bindForSimpleCounterIncrement(PropertyMeta counterMeta, Long increment) {
            daoContext.bindForSimpleCounterIncrement(daoFacade, counterMeta, increment);
        }

        public Long getSimpleCounter(PropertyMeta counterMeta, ConsistencyLevel consistency) {
            log.trace("Get counter value for counterMeta '{}' with consistency level '{}'", counterMeta, consistency);

            return daoContext.getSimpleCounter(daoFacade, counterMeta, consistency);
        }

        public void bindForSimpleCounterDeletion(PropertyMeta counterMeta) {
            daoContext.bindForSimpleCounterDelete(daoFacade, counterMeta);
        }

        // Clustered counter
        public void pushClusteredCounterIncrementStatement(PropertyMeta counterMeta, Long increment) {
            daoContext.pushClusteredCounterIncrementStatement(daoFacade, counterMeta, increment);
        }

        public ListenableFuture<Row> getClusteredCounter() {
            log.trace("Get clustered counter value for entityMeta '{}'", entityMeta);
            return daoContext.getClusteredCounter(daoFacade);
        }

        public Long getClusteredCounterColumn(PropertyMeta counterMeta) {
            log.trace("Get clustered counter value for counterMeta '{}'", counterMeta);
            return daoContext.getClusteredCounterColumn(daoFacade, counterMeta);
        }

        public void bindForClusteredCounterDeletion() {
            daoContext.bindForClusteredCounterDelete(daoFacade);
        }
    }

    public class DaoFacade extends StateHolderFacade implements DaoOperations {

        private DaoFacade() {
        }

        public void pushStatement(AbstractStatementWrapper statementWrapper) {
            flushContext.pushStatement(statementWrapper);
        }

        public void pushCounterStatement(AbstractStatementWrapper statementWrapper) {
            flushContext.pushCounterStatement(statementWrapper);
        }

        public ListenableFuture<ResultSet> executeImmediate(AbstractStatementWrapper bsWrapper) {
            return flushContext.execute(bsWrapper);
        }
    }
}
