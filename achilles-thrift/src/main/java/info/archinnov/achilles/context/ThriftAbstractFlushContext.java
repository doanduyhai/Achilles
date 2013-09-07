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

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ThriftAbstractFlushContext<T extends ThriftAbstractFlushContext<T>>
		extends FlushContext<T> {
	protected static final Logger log = LoggerFactory
			.getLogger(ThriftAbstractFlushContext.class);

	protected ThriftDaoContext thriftDaoContext;
	protected ThriftConsistencyContext consistencyContext;

	protected Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap = new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>();
	protected boolean hasCustomConsistencyLevels = false;
	protected ConsistencyLevel consistencyLevel;

	protected ThriftAbstractFlushContext(ThriftDaoContext thriftDaoContext,
			AchillesConsistencyLevelPolicy policy,
			ConsistencyLevel consistencyLevel) {
		this.thriftDaoContext = thriftDaoContext;
		this.consistencyContext = new ThriftConsistencyContext(policy,
				consistencyLevel);
	}

	protected ThriftAbstractFlushContext(ThriftDaoContext thriftDaoContext,
			ThriftConsistencyContext consistencyContext,
			Map<String, Pair<Mutator<Object>, ThriftAbstractDao>> mutatorMap,
			boolean hasCustomConsistencyLevels) {
		this.thriftDaoContext = thriftDaoContext;
		this.consistencyContext = consistencyContext;
		this.mutatorMap = mutatorMap;
		this.hasCustomConsistencyLevels = hasCustomConsistencyLevels;
	}

	protected void doFlush() {
		log.debug("Execute mutations flush");
		try {
			for (Entry<String, Pair<Mutator<Object>, ThriftAbstractDao>> entry : mutatorMap
					.entrySet()) {
				ThriftAbstractDao dao = entry.getValue().right;
				Mutator<?> mutator = entry.getValue().left;
				dao.executeMutator(mutator);
			}
		} finally {
			cleanUp();
		}
	}

	@Override
	public void cleanUp() {
		log.debug("Cleaning up flush context");
		consistencyContext.reinitConsistencyLevels();
		hasCustomConsistencyLevels = false;
		mutatorMap.clear();
	}

	@Override
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		hasCustomConsistencyLevels = true;
		consistencyContext.setConsistencyLevel(consistencyLevel);
	}

	public void reinitConsistencyLevels() {
		if (!hasCustomConsistencyLevels) {
			consistencyContext.reinitConsistencyLevels();
		}
	}

	public Mutator<Object> getEntityMutator(String tableName) {
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(tableName)) {
			mutator = mutatorMap.get(tableName).left;
		} else {
			ThriftGenericEntityDao entityDao = thriftDaoContext
					.findEntityDao(tableName);

			if (entityDao != null) {
				mutator = entityDao.buildMutator();
				mutatorMap.put(tableName, Pair
						.<Mutator<Object>, ThriftAbstractDao> create(mutator,
								entityDao));
			}
		}
		return mutator;
	}

	public Mutator<Object> getWideRowMutator(String tableName) {
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(tableName)) {
			mutator = mutatorMap.get(tableName).left;
		} else {
			ThriftGenericWideRowDao columnFamilyDao = thriftDaoContext
					.findWideRowDao(tableName);

			if (columnFamilyDao != null) {
				mutator = columnFamilyDao.buildMutator();
				mutatorMap.put(tableName, Pair
						.<Mutator<Object>, ThriftAbstractDao> create(mutator,
								columnFamilyDao));
			}
		}
		return mutator;
	}

	public Mutator<Object> getCounterMutator() {
		Mutator<Object> mutator = null;
		if (mutatorMap.containsKey(AchillesCounter.THRIFT_COUNTER_CF)) {
			mutator = mutatorMap.get(AchillesCounter.THRIFT_COUNTER_CF).left;
		} else {
			ThriftCounterDao thriftCounterDao = thriftDaoContext
					.getCounterDao();
			mutator = thriftCounterDao.buildMutator();
			mutatorMap.put(AchillesCounter.THRIFT_COUNTER_CF, Pair
					.<Mutator<Object>, ThriftAbstractDao> create(mutator,
							thriftCounterDao));
		}
		return mutator;
	}

	public ThriftConsistencyContext getConsistencyContext() {
		return consistencyContext;
	}

	@Override
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyContext.getConsistencyLevel();
	}
}
