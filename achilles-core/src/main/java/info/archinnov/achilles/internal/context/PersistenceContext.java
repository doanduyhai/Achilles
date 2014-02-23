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
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.interceptor.Event.*;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
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

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class PersistenceContext {

	private static final Logger log = LoggerFactory.getLogger(PersistenceContext.class);

	protected AbstractFlushContext flushContext;
	protected EntityInitializer initializer = new EntityInitializer();
	protected EntityPersister persister = new EntityPersister();
	protected EntityProxifier proxifier = new EntityProxifier();
	protected EntityRefresher refresher = new EntityRefresher();
	protected EntityLoader loader = new EntityLoader();
	protected EntityUpdater updater = new EntityUpdater();

	protected ConfigurationContext configContext;
	protected Class<?> entityClass;
	protected EntityMeta entityMeta;
	protected Object entity;
	protected Object primaryKey;
	protected Object partitionKey;

	protected Options options = OptionsBuilder.noOptions();

	protected DaoContext daoContext;

	private ConsistencyOverrider overrider = new ConsistencyOverrider();
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
		this.primaryKey = entityMeta.getPrimaryKey(entity);
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

	public Row loadEntity() {
		return daoContext.loadEntity(this);
	}

	public Row loadProperty(PropertyMeta pm) {
		return daoContext.loadProperty(this, pm);
	}

	public void pushInsertStatement() {
		daoContext.pushInsertStatement(this);
	}

	public void pushUpdateStatement(List<PropertyMeta> pms) {
		daoContext.pushUpdateStatement(this, pms);
	}

    public void pushCollectionAndMapUpdateStatements(DirtyCheckChangeSet changeSet) {
        daoContext.pushCollectionAndMapUpdateStatement(this, changeSet);
    }

	public void bindForRemoval(String tableName) {
		daoContext.bindForRemoval(this, tableName);
	}

	// Simple counter
	public void bindForSimpleCounterIncrement(PropertyMeta counterMeta, Long increment) {
		daoContext.bindForSimpleCounterIncrement(this, entityMeta, counterMeta, increment);
	}

	public void incrementSimpleCounter(PropertyMeta counterMeta, Long increment, ConsistencyLevel consistency) {
		daoContext.incrementSimpleCounter(this, entityMeta, counterMeta, increment, consistency);
	}

	public void decrementSimpleCounter(PropertyMeta counterMeta, Long decrement, ConsistencyLevel consistency) {
		daoContext.decrementSimpleCounter(this, entityMeta, counterMeta, decrement, consistency);
	}

	public Long getSimpleCounter(PropertyMeta counterMeta, ConsistencyLevel consistency) {
		log.trace("Get counter value for counterMeta '{}' with consistency level '{}'", counterMeta, consistency);

		Row row = daoContext.getSimpleCounter(this, counterMeta, consistency);
		if (row != null) {
			return row.getLong(CQL_COUNTER_VALUE);
		}
		return null;
	}

	public void bindForSimpleCounterRemoval(PropertyMeta counterMeta) {
		daoContext.bindForSimpleCounterDelete(this, entityMeta, counterMeta, primaryKey);
	}

	// Clustered counter
	public void pushClusteredCounterIncrementStatement(PropertyMeta counterMeta, Long increment) {
		daoContext.pushClusteredCounterIncrementStatement(this, entityMeta, counterMeta, increment);
	}

	public Row getClusteredCounter(ConsistencyLevel readLevel) {
		log.trace("Get clustered counter value for entityMeta '{}' with consistency level '{}'", entityMeta, readLevel);
		return daoContext.getClusteredCounter(this, readLevel);
	}

	public Long getClusteredCounterColumn(PropertyMeta counterMeta, ConsistencyLevel readLevel) {
		log.trace("Get clustered counter value for counterMeta '{}' with consistency level '{}'", counterMeta,
				readLevel);
		return daoContext.getClusteredCounterColumn(this, counterMeta, readLevel);
	}

	public void bindForClusteredCounterRemoval() {
		daoContext.bindForClusteredCounterDelete(this, entityMeta, primaryKey);
	}

	public ResultSet bindAndExecute(PreparedStatement ps, Object... params) {
		return daoContext.bindAndExecute(ps, params);
	}

	public void pushStatement(AbstractStatementWrapper statementWrapper) {
		flushContext.pushStatement(statementWrapper);
	}

	public void pushCounterStatement(AbstractStatementWrapper statementWrapper) {
		flushContext.pushCounterStatement(statementWrapper);
	}

	public ResultSet executeImmediate(AbstractStatementWrapper bsWrapper) {
		return flushContext.executeImmediate(bsWrapper);
	}

	public <T> T persist(T rawEntity) {
		flushContext.triggerInterceptor(entityMeta, rawEntity, PRE_PERSIST);
		persister.persist(this);
		flush();
		flushContext.triggerInterceptor(entityMeta, rawEntity, POST_PERSIST);
		return proxifier.buildProxyWithAllFieldsLoadedExceptCounters(rawEntity, this);
	}

	public void update(Object proxifiedEntity) {
		flushContext.triggerInterceptor(entityMeta, entity, PRE_UPDATE);
		updater.update(this, proxifiedEntity);
		flush();
		flushContext.triggerInterceptor(entityMeta, entity, POST_UPDATE);
	}

	public void remove() {
		flushContext.triggerInterceptor(entityMeta, entity, PRE_REMOVE);
		persister.remove(this);
		flush();
		flushContext.triggerInterceptor(entityMeta, entity, POST_REMOVE);
	}

	public <T> T find(Class<T> entityClass) {
		T rawEntity = loader.load(this, entityClass);
		T proxifiedEntity = null;
		if (rawEntity != null) {
			flushContext.triggerInterceptor(entityMeta, rawEntity, POST_LOAD);
			proxifiedEntity = proxifier.buildProxyWithAllFieldsLoadedExceptCounters(rawEntity, this);
		}
		return proxifiedEntity;
	}

	public <T> T getProxy(Class<T> entityClass) {
		T entity = loader.createEmptyEntity(this, entityClass);
		return proxifier.buildProxyWithNoFieldLoaded(entity, this);
	}

	public void refresh(Object proxifiedEntity) throws AchillesStaleObjectStateException {
		refresher.refresh(proxifiedEntity, this);
		flushContext.triggerInterceptor(entityMeta, entity, POST_LOAD);
	}

	public <T> T initialize(T proxifiedEntity) {
		initializer.initializeEntity(proxifiedEntity, entityMeta);
		return proxifiedEntity;
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

	public PropertyMeta getIdMeta() {
		return entityMeta.getIdMeta();
	}

	public boolean isClusteredEntity() {
		return this.entityMeta.isClusteredEntity();
	}

	public boolean isClusteredCounter() {
		return this.entityMeta.isClusteredCounter();
	}

	public String getTableName() {
		return entityMeta.getTableName();
	}

	public boolean isBatchMode() {
		return flushContext.type() == AbstractFlushContext.FlushType.BATCH;
	}

	public void flush() {
		flushContext.flush();
	}

	public void endBatch() {
		flushContext.endBatch();
	}

	public EntityMeta getEntityMeta() {
		return entityMeta;
	}

	public Object getEntity() {
		return entity;
	}

	public void setEntity(Object entity) {
		this.entity = entity;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> getEntityClass() {
		return (Class<T>) entityClass;
	}

	public Object getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(Object primaryKey) {
		this.primaryKey = primaryKey;
	}

	public Object getPartitionKey() {
		if (partitionKey == null && primaryKey != null) {
			extractPartitionKey();
		}
		return partitionKey;
	}

	public void setPartitionKey(Object partitionKey) {
		this.partitionKey = partitionKey;
	}

	public ConfigurationContext getConfigContext() {
		return configContext;
	}

	public void setEntityMeta(EntityMeta entityMeta) {
		this.entityMeta = entityMeta;
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

	public Set<Method> getAllGetters() {
		return new HashSet<>(from(entityMeta.getAllMetas()).transform(metaToGetter).toList());
	}

	public Set<Method> getAllGettersExceptCounters() {
		return new HashSet<>(from(entityMeta.getAllMetasExceptCounters()).transform(metaToGetter).toList());
	}

	public List<PropertyMeta> getAllCountersMeta() {
		return entityMeta.getAllCounterMetas();
	}

	private void extractPartitionKey() {
		if (entityMeta.hasEmbeddedId()) {
			this.partitionKey = entityMeta.getPartitionKey(primaryKey);
		}
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(PersistenceContext.class).add("entity class", this.entityClass)
				.add("primary key", this.primaryKey).add("partition key", this.partitionKey).toString();
	}
}
