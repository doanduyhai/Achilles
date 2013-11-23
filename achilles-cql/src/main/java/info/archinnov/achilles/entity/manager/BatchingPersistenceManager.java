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

import info.archinnov.achilles.context.BatchingFlushContext;
import info.archinnov.achilles.context.DaoContext;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.context.PersistenceContextFactory;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchingPersistenceManager extends PersistenceManager {

	private static final Logger log = LoggerFactory.getLogger(BatchingPersistenceManager.class);

	private BatchingFlushContext flushContext;

	BatchingPersistenceManager(Map<Class<?>, EntityMeta> entityMetaMap, PersistenceContextFactory contextFactory,
                               DaoContext daoContext, ConfigurationContext configContext) {
		super(entityMetaMap, contextFactory, daoContext, configContext);
		this.flushContext = new BatchingFlushContext(daoContext, null);
	}

	/**
	 * Start a batch session.
	 */
	public void startBatch() {
		log.debug("Starting batch mode");
		flushContext.startBatch();
	}

	/**
	 * Start a batch session with read/write consistency levels
	 */
	public void startBatch(ConsistencyLevel consistencyLevel) {
		log.debug("Starting batch mode with consistency level {}", consistencyLevel.name());
		startBatch();
		flushContext.setConsistencyLevel(consistencyLevel);

	}

	/**
	 * End an existing batch and flush all the pending statements.
	 * 
	 * Do nothing if there is no pending statement
	 * 
	 */
	public void endBatch() {
		log.debug("Ending batch mode");
		try {
			flushContext.endBatch();
		} finally {
			flushContext.cleanUp();
		}
	}

	/**
	 * Cleaning all pending statements for the current batch session.
	 */
	public void cleanBatch() {
		log.debug("Cleaning all pending statements");
		flushContext.cleanUp();
	}

	@Override
	public void persist(final Object entity, Options options) {
		if (options.getConsistencyLevel().isPresent()) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");
		} else {
			super.persist(entity, options);
		}
	}

	@Override
	public <T> T merge(final T entity, Options options) {
		if (options.getConsistencyLevel().isPresent()) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");
		} else {
			return super.merge(entity, options);
		}
	}

	@Override
	public void remove(final Object entity, ConsistencyLevel writeLevel) {
		if (writeLevel != null) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");
		} else {
			super.remove(entity, null);
		}
	}

	@Override
	public <T> T find(final Class<T> entityClass, final Object primaryKey, ConsistencyLevel readLevel) {
		if (readLevel != null) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");
		} else {
			return super.find(entityClass, primaryKey, null);
		}
	}

	@Override
	public <T> T getReference(final Class<T> entityClass, final Object primaryKey, ConsistencyLevel readLevel) {
		if (readLevel != null) {
			flushContext.cleanUp();
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");
		} else {
			return super.getReference(entityClass, primaryKey, null);
		}
	}

	@Override
	public void refresh(final Object entity, ConsistencyLevel readLevel) throws AchillesStaleObjectStateException {
		if (readLevel != null) {
			throw new AchillesException(
					"Runtime custom Consistency Level cannot be set for batch mode. Please set the Consistency Levels at batch start with 'startBatch(consistencyLevel)'");
		} else {
			super.refresh(entity, null);
		}
	}

    @Override
	protected PersistenceContext initPersistenceContext(Class<?> entityClass, Object primaryKey, Options options) {
		log.trace("Initializing new persistence context for entity class {} and primary key {}",
				entityClass.getCanonicalName(), primaryKey);
        return contextFactory.newContextWithFlushContext(entityClass,primaryKey,options,flushContext);
	}

    @Override
	protected PersistenceContext initPersistenceContext(Object entity, Options options) {
		log.trace("Initializing new persistence context for entity {}", entity);
        return contextFactory.newContextWithFlushContext(entity,options,flushContext);
	}
}
