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

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;

public class ImmediateFlushContext extends AbstractFlushContext {
	private static final Logger log = LoggerFactory.getLogger(ImmediateFlushContext.class);

	public ImmediateFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel) {
		super(daoContext, consistencyLevel);
	}

	private ImmediateFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
			ConsistencyLevel consistencyLevel) {
		super(daoContext, statementWrappers, consistencyLevel);
	}

	@Override
	public void startBatch() {
		throw new UnsupportedOperationException(
				"Cannot start a batch with a normal PersistenceManager. Please create a BatchingPersistenceManager instead");
	}

	@Override
	public void endBatch() {
		throw new UnsupportedOperationException(
				"Cannot end a batch with a normal PersistenceManager. Please create a BatchingPersistenceManager instead");
	}

	@Override
	public void flush() {
		log.debug("Flush immediately all pending statements");
        executeBatch(BatchStatement.Type.UNLOGGED, statementWrappers);
        executeBatch(BatchStatement.Type.COUNTER, counterStatementWrappers);
	}

	@Override
	public FlushType type() {
		return FlushType.IMMEDIATE;
	}

	@Override
	public ImmediateFlushContext duplicate() {
		log.trace("Duplicate immediate flushing context");
		return new ImmediateFlushContext(daoContext, statementWrappers, consistencyLevel);
	}

    @Override
    public void triggerInterceptor(EntityMeta meta, Object entity, Event event) {
        meta.intercept(entity,event);
    }
}
