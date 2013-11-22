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

import info.archinnov.achilles.statement.prepared.BoundStatementWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CQLBatchingFlushContext extends CQLAbstractFlushContext {
	private static final Logger log = LoggerFactory.getLogger(CQLBatchingFlushContext.class);

	public CQLBatchingFlushContext(CQLDaoContext daoContext, ConsistencyLevel consistencyLevel) {
		super(daoContext, consistencyLevel);
	}

	private CQLBatchingFlushContext(CQLDaoContext daoContext, List<BoundStatementWrapper> boundStatementWrappers,
			ConsistencyLevel consistencyLevel) {
		super(daoContext, boundStatementWrappers, consistencyLevel);
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
		doFlush();
	}

	@Override
	public FlushType type() {
		return FlushType.BATCH;
	}

	@Override
	public CQLBatchingFlushContext duplicate() {
		return new CQLBatchingFlushContext(daoContext, boundStatementWrappers, consistencyLevel);
	}

}
