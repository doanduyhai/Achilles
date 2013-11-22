/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.context;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.CQLEntityInitializer;
import info.archinnov.achilles.entity.operations.CQLEntityLoader;
import info.archinnov.achilles.entity.operations.CQLEntityMerger;
import info.archinnov.achilles.entity.operations.CQLEntityPersister;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.entity.operations.CQLEntityRefresher;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;

public class CQLPersistenceContext {

	private CQLAbstractFlushContext flushContext;
	private CQLEntityInitializer initializer = new CQLEntityInitializer();
	private CQLEntityPersister persister = new CQLEntityPersister();
	private CQLEntityProxifier proxifier = new CQLEntityProxifier();
	private CQLEntityRefresher refresher = new CQLEntityRefresher();
	private CQLEntityLoader loader = new CQLEntityLoader();
	private CQLEntityMerger merger = new CQLEntityMerger();

	private ConfigurationContext configContext;
	private Class<?> entityClass;
	private EntityMeta entityMeta;
	private Object entity;
	private Object primaryKey;
	private Object partitionKey;

	private Options options = OptionsBuilder.noOptions();
	private boolean loadEagerFields = true;

	private CQLDaoContext daoContext;

	public CQLPersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, CQLDaoContext daoContext,
			CQLAbstractFlushContext flushContext, Class<?> entityClass, Object primaryKey, Options options) {
		Validator.validateNotNull(entityClass, "The entity class should not be null for persistence context creation");
		Validator.validateNotNull(primaryKey,
				"The primary key for the entity class '%s' should not be null for persistence context creation",
				entityClass.getCanonicalName());
		this.entityMeta = entityMeta;
		this.configContext = configContext;
		this.daoContext = daoContext;
		this.flushContext = flushContext;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
		this.options = options;
	}

	public CQLPersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, CQLDaoContext daoContext,
			CQLAbstractFlushContext flushContext, Object entity, Options options) {
		Validator.validateNotNull(entity,
				"The entity of type '%s' should not be null for persistence context creation",
				entityMeta.getClassName());
		this.primaryKey = entityMeta.getPrimaryKey(entity);
		Validator.validateNotNull(primaryKey,
				"The primary key for the entity class '%s' should not be null for persistence context creation",
				entityMeta.getClassName());

		this.entityClass = entityMeta.getEntityClass();
		this.entityMeta = entityMeta;
		this.configContext = configContext;
		this.daoContext = daoContext;
		this.flushContext = flushContext;
		this.entity = entity;
		this.options = options;
	}

	public CQLPersistenceContext duplicate(Object entity) {
		return new CQLPersistenceContext(entityMeta, configContext, daoContext, flushContext.duplicate(), entity,
				options.duplicateWithoutTtlAndTimestamp());
	}

	public Row eagerLoadEntity() {
		return daoContext.eagerLoadEntity(this);
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

	public void incrementClusteredCounter(PropertyMeta counterMeta, Long increment, ConsistencyLevel consistency) {
		daoContext.incrementClusteredCounter(this, entityMeta, counterMeta, increment, consistency);
	}

	public void decrementClusteredCounter(PropertyMeta counterMeta, Long decrement, ConsistencyLevel consistency) {
		daoContext.decrementClusteredCounter(this, entityMeta, counterMeta, decrement, consistency);
	}

	public Long getClusteredCounter(PropertyMeta counterMeta, ConsistencyLevel readLevel) {
		Row row = daoContext.getClusteredCounter(this, counterMeta, readLevel);
		if (row != null) {
			return row.getLong(counterMeta.getPropertyName());
		}
		return null;
	}

	public void bindForClusteredCounterRemoval(PropertyMeta counterMeta) {
		daoContext.bindForClusteredCounterDelete(this, entityMeta, counterMeta, primaryKey);
	}

	public ResultSet bindAndExecute(PreparedStatement ps, Object... params) {
		return daoContext.bindAndExecute(ps, params);
	}

	public void pushBoundStatement(BoundStatementWrapper bsWrapper, ConsistencyLevel writeLevel) {
		flushContext.pushBoundStatement(bsWrapper, writeLevel);
	}

	public void pushStatement(Statement statement, ConsistencyLevel writeLevel) {
		flushContext.pushStatement(statement, writeLevel);
	}

	public ResultSet executeImmediateWithConsistency(BoundStatementWrapper bsWrapper,
			ConsistencyLevel readConsistencyLevel) {
		return flushContext.executeImmediateWithConsistency(bsWrapper.getBs(), readConsistencyLevel,
				bsWrapper.getValues());
	}

	public void persist() {
		persister.persist(this);
		flush();
	}

	public <T> T merge(T entity) {
		T merged = merger.merge(this, entity);
		flush();
		return merged;
	}

	public void remove() {
		persister.remove(this);
		flush();
	}

	public <T> T find(Class<T> entityClass) {
		T entity = loader.<T> load(this, entityClass);

		if (entity != null) {
			entity = proxifier.buildProxy(entity, this);
		}
		return entity;
	}

	public <T> T getReference(Class<T> entityClass) {
		setLoadEagerFields(false);
		return find(entityClass);
	}

	public void refresh() throws AchillesStaleObjectStateException {
		refresher.refresh(this);
	}

	public <T> T initialize(T entity) {
		final CQLEntityInterceptor<T> interceptor = proxifier.getInterceptor(entity);
		initializer.initializeEntity(entity, entityMeta, interceptor);
		return entity;
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

	public PropertyMeta getFirstMeta() {
		return entityMeta.getAllMetasExceptIdMeta().get(0);
	}

	public boolean isClusteredEntity() {
		return this.entityMeta.isClusteredEntity();
	}

	public String getTableName() {
		return entityMeta.getTableName();
	}

	public boolean isBatchMode() {
		return flushContext.type() == CQLAbstractFlushContext.FlushType.BATCH;
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

	public Class<?> getEntityClass() {
		return entityClass;
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

	public boolean isLoadEagerFields() {
		return loadEagerFields;
	}

	public void setLoadEagerFields(boolean loadEagerFields) {
		this.loadEagerFields = loadEagerFields;
	}

	public Optional<Integer> getTtt() {
		return options.getTtl();
	}

	public Optional<Long> getTimestamp() {
		return options.getTimestamp();
	}

	public Optional<ConsistencyLevel> getConsistencyLevel() {
		return Optional.fromNullable(flushContext.getConsistencyLevel());
	}

	private void extractPartitionKey() {
		if (entityMeta.hasEmbeddedId()) {
			this.partitionKey = entityMeta.getPartitionKey(primaryKey);
		}
	}

	void setInitializer(CQLEntityInitializer initializer) {
		this.initializer = initializer;
	}

	void setPersister(CQLEntityPersister persister) {
		this.persister = persister;
	}

	void setProxifier(CQLEntityProxifier proxifier) {
		this.proxifier = proxifier;
	}

	void setRefresher(CQLEntityRefresher refresher) {
		this.refresher = refresher;
	}

	void setLoader(CQLEntityLoader loader) {
		this.loader = loader;
	}

	void setMerger(CQLEntityMerger merger) {
		this.merger = merger;
	}
}
