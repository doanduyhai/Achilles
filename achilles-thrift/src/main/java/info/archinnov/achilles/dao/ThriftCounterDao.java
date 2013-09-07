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
package info.archinnov.achilles.dao;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.COMPOSITE_SRZ;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.counter.AchillesCounter;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftCounterDao extends ThriftAbstractDao {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftCounterDao.class);

	public ThriftCounterDao(Cluster cluster, Keyspace keyspace,
			AchillesConsistencyLevelPolicy consistencyPolicy,
			Pair<?, ?> rowkeyAndValueClasses) {
		super(cluster, keyspace, AchillesCounter.THRIFT_COUNTER_CF,
				consistencyPolicy, rowkeyAndValueClasses);

		columnNameSerializer = COMPOSITE_SRZ;
		log.debug("Initializing CounterDao with Composite key serializer, DynamicComposite comparator and Long value serializer ");
	}
}
