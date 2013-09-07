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
package info.archinnov.achilles.entity.manager;

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftBatchingFlushContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.context.ThriftPersistenceContextFactory;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftBatchingEntityManager extends ThriftEntityManager {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftBatchingEntityManager.class);

	private ThriftBatchingFlushContext flushContext;

	ThriftBatchingEntityManager(Map<Class<?>, EntityMeta> entityMetaMap,
			ThriftPersistenceContextFactory contextFactory,
			ThriftDaoContext daoContext, ConfigurationContext configContext) {
		super(entityMetaMap, contextFactory, daoContext, configContext);
		this.flushContext = new ThriftBatchingFlushContext(daoContext,
				consistencyPolicy, null);
	}

	/**
	 * Start a batch session using a Hector mutator.
	 */
	public void startBatch() {
		log.debug("Starting batch mode");
		flushContext.startBatch();
	}

	/**
	 * Start a batch session with read/write consistency levels using a Hector
	 * mutator.
	 */
	public void startBatch(ConsistencyLevel consistencyLevel) {
		log.debug("Starting batch mode with write consistency level {}",
				consistencyLevel);
		startBatch();
		flushContext.setConsistencyLevel(consistencyLevel);

	}

	/**
	 * End an existing batch and flush all the mutators.
	 * 
	 * All join entities will be flushed through their own mutator.
	 * 
	 * Do nothing if no batch mutator was started
	 * 
	 */
	public void endBatch() {
		log.debug("Ending batch mode");
		flushContext.endBatch();
	}

	/**
	 * Cleaning all pending mutations for the current batch session.
	 */
	public void cleanBatch() {
		log.debug("Cleaning all pending mutations");
		flushContext.cleanUp();
	}

	@Override
	public void persist(final Object entity, Options options) {
		if (options.getConsistencyLevel().isPresent()) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
		} else {
			super.persist(entity, options);
		}
	}

	@Override
	public <T> T merge(final T entity, Options options) {
		if (options.getConsistencyLevel().isPresent()) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
		} else {
			return super.merge(entity, options);
		}
	}

	@Override
	public void remove(final Object entity, ConsistencyLevel writeLevel) {
		if (writeLevel != null) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
		} else {
			super.remove(entity, null);
		}
	}

	@Override
	public <T> T find(final Class<T> entityClass, final Object primaryKey,
			ConsistencyLevel readLevel) {
		if (readLevel != null) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
		} else {
			return super.find(entityClass, primaryKey, null);
		}
	}

	@Override
	public <T> T getReference(final Class<T> entityClass,
			final Object primaryKey, ConsistencyLevel readLevel) {
		if (readLevel != null) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
		} else {
			return super.getReference(entityClass, primaryKey, null);
		}
	}

	@Override
	public void refresh(final Object entity, ConsistencyLevel readLevel)
			throws AchillesStaleObjectStateException {
		if (readLevel != null) {
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(readLevel,writeLevel)'");
		} else {
			super.refresh(entity, null);
		}
	}

	@Override
	protected ThriftPersistenceContext initPersistenceContext(
			Class<?> entityClass, Object primaryKey, Options options) {
		log.trace(
				"Initializing new persistence context for entity class {} and primary key {}",
				entityClass.getCanonicalName(), primaryKey);

		EntityMeta entityMeta = entityMetaMap.get(entityClass);
		return new ThriftPersistenceContext(entityMeta, configContext,
				daoContext, flushContext, entityClass, primaryKey, options,
				new HashSet<String>());
	}

	@Override
	protected ThriftPersistenceContext initPersistenceContext(Object entity,
			Options options) {
		log.trace("Initializing new persistence context for entity {}", entity);

		EntityMeta entityMeta = this.entityMetaMap.get(proxifier
				.deriveBaseClass(entity));
		return new ThriftPersistenceContext(entityMeta, configContext,
				daoContext, flushContext, entity, options,
				new HashSet<String>());
	}
}
