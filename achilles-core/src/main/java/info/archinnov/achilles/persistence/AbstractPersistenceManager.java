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

package info.archinnov.achilles.persistence;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Statement;
import info.archinnov.achilles.internal.metadata.holder.EntityMetaConfig;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.type.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.context.facade.PersistenceManagerOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.persistence.operations.EntityValidator;
import info.archinnov.achilles.internal.persistence.operations.OptionsValidator;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.query.typed.TypedQueryValidator;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options;

abstract class AbstractPersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(AbstractPersistenceManager.class);

    protected Map<Class<?>, EntityMeta> entityMetaMap;
    protected ConfigurationContext configContext;
    protected PersistenceContextFactory contextFactory;

    protected EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();
    protected OptionsValidator optionsValidator = OptionsValidator.Singleton.INSTANCE.get();
    protected EntityValidator entityValidator = EntityValidator.Singleton.INSTANCE.get();
    protected TypedQueryValidator typedQueryValidator = TypedQueryValidator.Singleton.INSTANCE.get();

    protected SliceQueryExecutor sliceQueryExecutor;

    protected DaoContext daoContext;

    protected AbstractPersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, //
            PersistenceContextFactory contextFactory, DaoContext daoContext, ConfigurationContext configContext) {
        this.entityMetaMap = entityMetaMap;
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.contextFactory = contextFactory;
        this.sliceQueryExecutor = new SliceQueryExecutor(contextFactory, configContext, daoContext);
    }

    protected <T> AchillesFuture<T> asyncInsert(final T entity, Options options) {
        entityValidator.validateEntity(entity, entityMetaMap);
        optionsValidator.validateOptionsForUpsert(entity, entityMetaMap, options);
        proxifier.ensureNotProxy(entity);
        PersistenceManagerOperations context = initPersistenceContext(entity, options);
        return context.persist(entity);
    }

    protected <T> AchillesFuture<T> asyncUpdate(T entity, Options options) {
        proxifier.ensureProxy(entity);
        Object realObject = proxifier.getRealObject(entity);
        entityValidator.validateEntity(realObject, entityMetaMap);
        optionsValidator.validateOptionsForUpsert(entity, entityMetaMap, options);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        return context.update(entity);
    }

    protected <T> AchillesFuture<T> asyncInsertOrUpdate(T entity, Options options) {
        entityValidator.validateEntity(entity, entityMetaMap);
        if (proxifier.isProxy(entity)) {
            return this.asyncUpdate(entity, options);
        } else {
            return this.asyncInsert(entity, options);
        }
    }


    protected <T> AchillesFuture<T> asyncDelete(final T entity, Options options) {
        Object realObject = proxifier.getRealObject(entity);
        entityValidator.validateEntity(realObject, entityMetaMap);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        return context.delete();
    }


    protected AchillesFuture<Empty> asyncDeleteById(Class<?> entityClass, Object primaryKey, Options options) {
        Validator.validateNotNull(entityClass, "The entity class should not be null for removal by id");
        Validator.validateNotNull(primaryKey, "The primary key should not be null for removal by id");

        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.deleteById();
    }

    protected <T> AchillesFuture<T> asyncFind(final Class<T> entityClass, final Object primaryKey, Options options) {
        Validator.validateNotNull(entityClass, "Entity class should not be null for find by id");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for find by id");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());
        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.find(entityClass);
    }

    protected <T> T getProxyInternal(final Class<T> entityClass, final Object primaryKey, Options options) {
        Validator.validateNotNull(entityClass, "Entity class should not be null for get proxy");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for get proxy");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());

        optionsValidator.validateNoAsyncListener(options);
        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, options);
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.getProxy(entityClass);
    }

    protected <T> T getProxyForUpdateInternal(final Class<T> entityClass, final Object primaryKey) {
        Validator.validateNotNull(entityClass, "Entity class should not be null for get proxy for update");
        Validator.validateNotNull(primaryKey, "Entity primaryKey should not be null for get proxy for update");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass), "The entity class '%s' is not managed by Achilles", entityClass.getCanonicalName());

        PersistenceManagerOperations context = initPersistenceContext(entityClass, primaryKey, noOptions());
        entityValidator.validatePrimaryKey(context.getIdMeta(), primaryKey);
        return context.getProxyForUpdate(entityClass);
    }

    protected <T> AchillesFuture<T> asyncRefresh(final T entity, Options options) throws AchillesStaleObjectStateException {
        proxifier.ensureProxy(entity);
        Object realObject = proxifier.getRealObject(entity);
        entityValidator.validateEntity(realObject, entityMetaMap);
        PersistenceManagerOperations context = initPersistenceContext(realObject, options);
        return context.refresh(entity);
    }

    protected <T> T initialize(final T entity) {
        proxifier.ensureProxy(entity);
        T realObject = proxifier.getRealObject(entity);
        PersistenceManagerOperations context = initPersistenceContext(realObject, noOptions());
        return context.initialize(entity);
    }

    protected <T> T removeProxy(T proxy) {
        return proxifier.removeProxy(proxy);
    }

    protected <T> List<T> removeProxy(List<T> proxies) {
        return proxifier.removeProxy(proxies);
    }

    protected <T> Set<T> removeProxy(Set<T> proxies) {
        return proxifier.removeProxy(proxies);
    }

    protected <T> EntityMeta validateSliceQueryInternal(Class<T> entityClass) {
        Validator.validateNotNull(entityClass,"The entityClass should be provided for slice query");
        EntityMeta meta = entityMetaMap.get(entityClass);
        Validator.validateNotNull(meta, "The entity '%s' is not managed by achilles", entityClass.getName());
        Validator.validateTrue(meta.structure().isClusteredEntity(),"Cannot perform slice query on entity type '%s' because it is " + "not a clustered entity",meta.getClassName());
        return meta;
    }

    protected <T> EntityMeta typedQueryInternal(Class<T> entityClass, Statement statement, Object... boundValues) {
        log.debug("Execute typed query for entity class {}", entityClass);
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotNull(statement, "The regularStatement for typed query should not be null");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),"Cannot perform typed query because the entityClass '%s' is not managed by Achilles",entityClass.getCanonicalName());

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateTypedQuery(entityClass, statement, meta);
        return meta;
    }

    protected <T> EntityMeta rawTypedQueryInternal(Class<T> entityClass, Statement statement, Object... boundValues) {
        Validator.validateNotNull(entityClass, "The entityClass for typed query should not be null");
        Validator.validateNotNull(statement, "The regularStatement for typed query should not be null");
        Validator.validateTrue(entityMetaMap.containsKey(entityClass),"Cannot perform typed query because the entityClass '%s' is not managed by Achilles",entityClass.getCanonicalName());

        EntityMeta meta = entityMetaMap.get(entityClass);
        typedQueryValidator.validateRawTypedQuery(entityClass, statement, meta);
        return meta;
    }

    protected <T> Statement indexedQueryInternal(Class<T> entityClass, IndexCondition indexCondition) {
        EntityMeta entityMeta = entityMetaMap.get(entityClass);

        Validator.validateFalse(entityMeta.structure().isClusteredEntity(), "Index query is not supported for clustered entity. Please use typed query/native query");
        Validator.validateNotNull(indexCondition, "Index condition should not be null");

        entityMeta.forTranscoding().encodeIndexConditionValue(indexCondition);

        String indexColumnName = indexCondition.getColumnName();
        final EntityMetaConfig metaConfig = entityMeta.config();
        final Select.Where statement = select().from(metaConfig.getKeyspaceName(), metaConfig.getTableName()).where(eq(indexColumnName, bindMarker(indexColumnName)));
        return statement;
    }

    protected String serializeToJSON(Object entity) throws IOException {
        Validator.validateNotNull(entity, "Cannot serialize to JSON null entity");
        final ObjectMapper objectMapper = configContext.getMapperFor(entity.getClass());
        return objectMapper.writeValueAsString(entity);
    }

    protected <T> T deserializeFromJSON(Class<T> type, String serialized) throws IOException {
        Validator.validateNotNull(type, "Cannot deserialize from JSON if target type is null");
        final ObjectMapper objectMapper = configContext.getMapperFor(type);
        return objectMapper.readValue(serialized, type);
    }

    protected Session getNativeSession() {
        return daoContext.getSession();
    }

    protected PersistenceManagerOperations initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
        return contextFactory.newContext(entityClass, primaryKey, options).getPersistenceManagerFacade();
    }

    protected PersistenceManagerOperations initPersistenceContext(Object entity, Options options) {
        return contextFactory.newContext(entity, options).getPersistenceManagerFacade();
    }

    protected Map<Class<?>, EntityMeta> getEntityMetaMap() {
        return entityMetaMap;
    }

    protected ConfigurationContext getConfigContext() {
        return configContext;
    }

    protected void setEntityMetaMap(Map<Class<?>, EntityMeta> entityMetaMap) {
        this.entityMetaMap = entityMetaMap;
    }

    protected void setConfigContext(ConfigurationContext configContext) {
        this.configContext = configContext;
    }

}
