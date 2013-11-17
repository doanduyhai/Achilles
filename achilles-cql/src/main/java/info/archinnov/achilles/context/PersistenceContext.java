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

import java.util.List;
import java.util.Set;
import com.google.common.base.Optional;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityInitializer;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;
import info.archinnov.achilles.type.OptionsBuilder;
import info.archinnov.achilles.validation.Validator;

public abstract class PersistenceContext {
	protected ReflectionInvoker invoker = new ReflectionInvoker();
	protected EntityInitializer initializer = new EntityInitializer();
	protected ConfigurationContext configContext;
	protected Class<?> entityClass;
	protected EntityMeta entityMeta;
	protected Object entity;
	protected Object primaryKey;
	protected Object partitionKey;
	protected FlushContext<?> flushContext;

	protected Options options = OptionsBuilder.noOptions();
	protected boolean loadEagerFields = true;

	private PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, FlushContext<?> flushContext,
			Class<?> entityClass, Options options) {
		this.entityMeta = entityMeta;
		this.configContext = configContext;
		this.flushContext = flushContext;
		this.entityClass = entityClass;
		this.options = options;

	}

	protected PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, Object entity,
			FlushContext<?> flushContext, Options options) {
		this(entityMeta, configContext, flushContext, entityMeta.getEntityClass(), options);
		Validator.validateNotNull(entity, "The entity should not be null for persistence context creation");
		this.entity = entity;
		this.primaryKey = entityMeta.getPrimaryKey(entity);
		Validator.validateNotNull(primaryKey,
				"The primary key for the entity '%s' should not be null for persistence context creation", entity);

	}

	protected PersistenceContext(EntityMeta entityMeta, ConfigurationContext configContext, Class<?> entityClass,
			Object primaryKey, FlushContext<?> flushContext, Options options) {
		this(entityMeta, configContext, flushContext, entityClass, options);

		this.primaryKey = primaryKey;
		this.flushContext = flushContext;
		Validator.validateNotNull(primaryKey,
				"The primary key for the entity '%s' should not be null for persistence context creation", entity);

	}

	private void extractPartitionKey() {
		if (entityMeta.hasEmbeddedId()) {
			this.partitionKey = entityMeta.getPartitionKey(primaryKey);
		}
	}

	public abstract void persist();

	public abstract <T> T merge(T entity);

	public abstract void remove();

	public abstract <T> T find(Class<T> entityClass);

	public abstract <T> T getReference(Class<T> entityClass);

	public abstract <T> T initialize(T entity);

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

	public abstract void refresh() throws AchillesStaleObjectStateException;

	public abstract PersistenceContext duplicate(Object entity);

	public boolean isClusteredEntity() {
		return this.entityMeta.isClusteredEntity();
	}

	public String getTableName() {
		return entityMeta.getTableName();
	}

	public boolean isBatchMode() {
		return flushContext.type() == FlushType.BATCH;
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

	public void setFlushContext(FlushContext<?> flushContext) {
		this.flushContext = flushContext;
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
}
