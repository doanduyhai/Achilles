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

import info.archinnov.achilles.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchingFlushContext extends AbstractFlushContext {
	private static final Logger log = LoggerFactory.getLogger(BatchingFlushContext.class);

	public BatchingFlushContext(DaoContext daoContext, ConsistencyLevel consistencyLevel) {
		super(daoContext, consistencyLevel);
	}

	private BatchingFlushContext(DaoContext daoContext, List<AbstractStatementWrapper> statementWrappers,
			ConsistencyLevel consistencyLevel) {
		super(daoContext, statementWrappers, consistencyLevel);
	}

	@Override
	public void startBatch() {
		log.debug("Starting a new batch");
		super.cleanUp();
	}

	@Override
	public void flush() {
		log.debug("Flush called but do nothing. Flushing is done only at the end of the batch");
	}

	@Override
	public void endBatch() {
		log.debug("Ending current batch");
		/*
		 * Deactivate prepared statement batches until
		 * https://issues.apache.org/jira/browse/CASSANDRA-6426 is solved
		 */

		// BatchStatement batch = new BatchStatement();
		// writeDMLStartBatch();
		// for (AbstractStatementWrapper statementWrapper : statementWrappers) {
		// batch.add(statementWrapper.getStatement());
		// statementWrapper.logDMLStatement(true, "\t");
		// }
		// writeDMLEndBatch(consistencyLevel);
		// batch.setConsistencyLevel(ConsistencyConvertor.getCQLLevel(consistencyLevel));
		// daoContext.executeBatch(batch);
		for (AbstractStatementWrapper statementWrapper : statementWrappers) {
			daoContext.execute(statementWrapper);
		}
		cleanUp();
	}

	@Override
	public FlushType type() {
		return FlushType.BATCH;
	}

	@Override
	public BatchingFlushContext duplicate() {
		return new BatchingFlushContext(daoContext, statementWrappers, consistencyLevel);
	}

}
