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

import static info.archinnov.achilles.context.FlushContext.FlushType.IMMEDIATE;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Map;

import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftImmediateFlushContext extends
		ThriftAbstractFlushContext<ThriftImmediateFlushContext> {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftImmediateFlushContext.class);

	public ThriftImmediateFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy,
			ConsistencyLevel consistencyLevel) {
		super(thriftDaoContext, policy, consistencyLevel);
	}

	public ThriftImmediateFlushContext(ThriftDaoContext thriftDaoContext,
			ThriftConsistencyContext consistencyContext,
			Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap,
			boolean hasCustomConsistencyLevels) {
		super(thriftDaoContext, consistencyContext, mutatorMap,
				hasCustomConsistencyLevels);
	}

	@Override
	public void startBatch() {
		throw new UnsupportedOperationException(
				"Cannot start a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
	}

	@Override
	public void flush() {
		log.debug("Flush immediatly all pending mutations");
		doFlush();
	}

	@Override
	public void endBatch() {
		throw new UnsupportedOperationException(
				"Cannot end a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
	}

	@Override
	public FlushType type() {
		return IMMEDIATE;
	}

	@Override
	public ThriftImmediateFlushContext duplicate() {
		return new ThriftImmediateFlushContext(thriftDaoContext,
				consistencyContext, mutatorMap, hasCustomConsistencyLevels);
	}
}
