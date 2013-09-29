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

import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftDaoFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftDaoContextBuilder {
	private static final Logger log = LoggerFactory.getLogger(ThriftDaoContextBuilder.class);

	private ThriftDaoFactory daoFactory = new ThriftDaoFactory();

	public ThriftDaoContext buildDao(Cluster cluster, Keyspace keyspace, Map<Class<?>, EntityMeta> entityMetaMap,
			ConfigurationContext configContext, boolean hasSimpleCounter) {

		Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
		Map<String, ThriftGenericWideRowDao> wideRowDaosMap = new HashMap<String, ThriftGenericWideRowDao>();
		ThriftCounterDao thriftCounterDao = null;
		if (hasSimpleCounter) {
			thriftCounterDao = daoFactory.createCounterDao(cluster, keyspace, configContext);
			log.debug("Build achillesCounterCF dao");
		}

		for (EntityMeta entityMeta : entityMetaMap.values()) {
			if (entityMeta.isClusteredEntity()) {
				daoFactory.createClusteredEntityDao(cluster, keyspace, configContext, entityMeta, wideRowDaosMap);
			} else {
				daoFactory.createDaosForEntity(cluster, keyspace, configContext, entityMeta, entityDaosMap,
						wideRowDaosMap);
			}
		}
		return new ThriftDaoContext(entityDaosMap, wideRowDaosMap, thriftCounterDao);
	}
}
