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

import static info.archinnov.achilles.helper.PropertyHelper.*;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.type.Counter;

import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftDaoFactory {

	private static final Logger log = LoggerFactory.getLogger(ThriftDaoFactory.class);

	public void createDaosForEntity(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext,
			EntityMeta entityMeta, Map<String, ThriftGenericEntityDao> entityDaosMap,
			Map<String, ThriftGenericWideRowDao> wideRowDaosMap) {

		createEntityDao(cluster, keyspace, configContext, entityMeta, entityDaosMap);

	}

	private void createEntityDao(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext,
			EntityMeta entityMeta, Map<String, ThriftGenericEntityDao> entityDaosMap) {
		String tableName = entityMeta.getTableName();

		Class<?> partitionKeyClass;
		if (entityMeta.hasCompositePartitionKey()) {
			partitionKeyClass = Composite.class;
		} else {
			partitionKeyClass = entityMeta.getIdClass();
		}

		ThriftGenericEntityDao entityDao = new ThriftGenericEntityDao(//
				cluster, //
				keyspace, //
				tableName, //
				configContext.getConsistencyPolicy(), //
				Pair.create(partitionKeyClass, String.class));
		entityDaosMap.put(tableName, entityDao);
		log.debug("Build entity dao for column family {}", tableName);
	}

	public void createClusteredEntityDao(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext,
			EntityMeta entityMeta, Map<String, ThriftGenericWideRowDao> wideRowDaosMap) {

		Class<?> partitionKeyClass;
		if (entityMeta.hasCompositePartitionKey()) {
			partitionKeyClass = Composite.class;
		} else {
			partitionKeyClass = entityMeta.getIdMeta().getPartitionComponentClasses().get(0);
		}

		Class<?> valueClass;
		if (entityMeta.isValueless()) {
			valueClass = String.class;
		} else {
			valueClass = entityMeta.getFirstMeta().getValueClass();
		}

		ThriftGenericWideRowDao dao;

		String tableName = entityMeta.getTableName();
		AchillesConsistencyLevelPolicy consistencyPolicy = configContext.getConsistencyPolicy();
		if (isSupportedType(valueClass)) {
			dao = new ThriftGenericWideRowDao(cluster, keyspace, //
					tableName, consistencyPolicy, //
					Pair.create(partitionKeyClass, valueClass));
		} else if (Counter.class.isAssignableFrom(valueClass)) {
			dao = new ThriftGenericWideRowDao(cluster, keyspace, //
					tableName, consistencyPolicy,//
					Pair.create(partitionKeyClass, Long.class));
		} else {
			dao = new ThriftGenericWideRowDao(cluster, keyspace, //
					tableName, consistencyPolicy, //
					Pair.create(partitionKeyClass, String.class));
		}
		wideRowDaosMap.put(tableName, dao);
		log.debug("Build clustered entity dao for column family {}", tableName);
	}

	public ThriftCounterDao createCounterDao(Cluster cluster, Keyspace keyspace, ConfigurationContext configContext) {
		ThriftCounterDao counterDao = new ThriftCounterDao(cluster, keyspace, configContext.getConsistencyPolicy(), //
				Pair.create(Composite.class, Long.class));
		log.debug("Build achillesCounterCF dao");

		return counterDao;
	}
}
