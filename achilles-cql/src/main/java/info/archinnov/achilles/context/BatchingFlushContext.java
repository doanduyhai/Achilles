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

import info.archinnov.achilles.consistency.ConsistencyConverter;
import info.archinnov.achilles.statement.wrapper.AbstractStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BatchStatement;

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
	public void startBatch(ConsistencyLevel defaultConsistencyLevel) {
		log.debug("Starting a new batch");
		this.cleanUp(defaultConsistencyLevel);
    }

	@Override
	public void flush() {
		log.debug("Flush called but do nothing. Flushing is done only at the end of the batch");
	}

	@Override
	public void endBatch(ConsistencyLevel defaultConsistencyLevel) {
		log.debug("Ending current batch");
		/*
		 * Deactivate prepared statement batches until
		 * https://issues.apache.org/jira/browse/CASSANDRA-6426 is solved
		 */

		BatchStatement batch = new BatchStatement();
        AbstractStatementWrapper.writeDMLStartBatch();
		for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            batch.add(statementWrapper.getStatement());
            statementWrapper.logDMLStatement(true, "\t");
		}
        AbstractStatementWrapper.writeDMLEndBatch(consistencyLevel);
        batch.setConsistencyLevel(ConsistencyConverter.getCQLLevel(consistencyLevel));
		daoContext.executeBatch(batch);
//		for (AbstractStatementWrapper statementWrapper : statementWrappers) {
//			daoContext.execute(statementWrapper);
//		}
		this.cleanUp(defaultConsistencyLevel);
	}

    public void cleanUp(ConsistencyLevel defaultConsistencyLevel) {
        super.cleanUp();
        super.consistencyLevel = defaultConsistencyLevel;
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
