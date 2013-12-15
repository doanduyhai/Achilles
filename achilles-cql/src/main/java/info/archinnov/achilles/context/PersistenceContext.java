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

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.operations.EntityRefresher;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class PersistenceContext {

	private static final Logger log = LoggerFactory.getLogger(PersistenceContext.class);

	private AbstractFlushContext flushContext;
	private EntityInitializer initializer = new EntityInitializer();
	private EntityPersister persister = new EntityPersister();
	private EntityProxifier proxifier = new EntityProxifier();
	private EntityRefresher refresher = new EntityRefresher();
	private EntityLoader loader = new EntityLoader();
	private EntityMerger merger = new EntityMerger();

	private ConfigurationContext configContext;
	private Class<?> entityClass;
	private EntityMeta entityMeta;
	private Object entity;
	private Object primaryKey;
	private Object partitionKey;

	private Options options = OptionsBuilder.noOptions();
	private boolean loadEagerFields = true;

	private DaoContext daoContext;

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
		this.options = options;
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
		this.options = options;
	}

	public PersistenceContext duplicate(Object entity) {
		log.trace("Duplicate PersistenceContext for entity '{}'", entity);

		return new PersistenceContext(entityMeta, configContext, daoContext, flushContext.duplicate(), entity,
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

	public void incrementClusteredCounter(Long increment, ConsistencyLevel consistency) {
		daoContext.incrementClusteredCounter(this, entityMeta, increment, consistency);
	}

	public void decrementClusteredCounter(Long decrement, ConsistencyLevel consistency) {
		daoContext.decrementClusteredCounter(this, entityMeta, decrement, consistency);
	}

	public Long getClusteredCounter(PropertyMeta counterMeta, ConsistencyLevel readLevel) {
		log.trace("Get clustered counter value for counterMeta '{}' with consistency level '{}'", counterMeta,
				readLevel);

		Row row = daoContext.getClusteredCounter(this, readLevel);
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

	public void pushStatement(AbstractStatementWrapper statementWrapper) {
		flushContext.pushStatement(statementWrapper);
	}

	public ResultSet executeImmediate(AbstractStatementWrapper bsWrapper) {
		return flushContext.executeImmediate(bsWrapper);
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
		T entity = loader.load(this, entityClass);

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
		final EntityInterceptor<T> interceptor = proxifier.getInterceptor(entity);
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
		return flushContext.type() == AbstractFlushContext.FlushType.BATCH;
	}

	public void flush() {
		flushContext.flush();
	}

	public void endBatch() {
		flushContext.endBatch(configContext.getDefaultWriteConsistencyLevel());
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

	@Override
	public String toString() {
		return Objects.toStringHelper(PersistenceContext.class).add("entity class", this.entityClass)
				.add("primary key", this.primaryKey).add("partition key", this.partitionKey).toString();
	}
}
