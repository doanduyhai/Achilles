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

import static info.archinnov.achilles.context.FlushContext.FlushType.BATCH;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Map;

import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftBatchingFlushContext extends
		ThriftAbstractFlushContext<ThriftBatchingFlushContext> {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftImmediateFlushContext.class);

	public ThriftBatchingFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy,
			ConsistencyLevel consistencyLevel) {
		super(thriftDaoContext, policy, consistencyLevel);
	}

	public ThriftBatchingFlushContext(ThriftDaoContext thriftDaoContext,
			ThriftConsistencyContext consistencyContext,
			Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap,
			boolean hasCustomConsistencyLevels) {
		super(thriftDaoContext, consistencyContext, mutatorMap,
				hasCustomConsistencyLevels);
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
		consistencyContext.setConsistencyLevel();
		doFlush();
	}

	@Override
	public FlushType type() {
		return BATCH;

	}

	@Override
	public ThriftBatchingFlushContext duplicate() {
		return new ThriftBatchingFlushContext(thriftDaoContext,
				consistencyContext, mutatorMap, hasCustomConsistencyLevels);
	}
}
