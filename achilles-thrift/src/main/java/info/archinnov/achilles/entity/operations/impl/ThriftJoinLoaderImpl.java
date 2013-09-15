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
package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftJoinEntityLoader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftJoinLoaderImpl extends ThriftLoaderImpl {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftJoinLoaderImpl.class);

	private ThriftJoinEntityLoader joinHelper = new ThriftJoinEntityLoader();

	public List<Object> loadJoinListProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {

		EntityMeta joinMeta = propertyMeta.joinMeta();
		List<Pair<Composite, String>> columns = fetchColumns(context,
				propertyMeta);

		List<Object> joinIds = new ArrayList<Object>();
		for (Pair<Composite, String> pair : columns) {
			int index = Integer.parseInt(pair.left.get(2, STRING_SRZ));
			joinIds.add(index, propertyMeta.forceDecodeFromJSON(pair.right));
		}
		log.trace("Loading join entities of class {} having primary keys {}",
				propertyMeta.getValueClass().getCanonicalName(), joinIds);

		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta
				.getTableName());
		List<Object> joinEntities = new ArrayList<Object>();
		fillCollectionWithJoinEntities(propertyMeta, joinMeta, joinIds,
				joinEntityDao, joinEntities);

		return joinEntities;
	}

	public Set<Object> loadJoinSetProperty(ThriftPersistenceContext context,
			PropertyMeta propertyMeta) {
		EntityMeta joinMeta = propertyMeta.joinMeta();

		List<Pair<Composite, String>> columns = fetchColumns(context,
				propertyMeta);

		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta
				.getTableName());

		List<Object> joinIds = new ArrayList<Object>();
		for (Pair<Composite, String> pair : columns) {
			joinIds.add(propertyMeta.forceDecodeFromJSON(pair.left.get(2,
					STRING_SRZ)));
		}
		Set<Object> joinEntities = new HashSet<Object>();
		fillCollectionWithJoinEntities(propertyMeta, joinMeta, joinIds,
				joinEntityDao, joinEntities);

		return joinEntities;
	}

	public Map<Object, Object> loadJoinMapProperty(
			ThriftPersistenceContext context, PropertyMeta propertyMeta) {

		EntityMeta joinMeta = propertyMeta.joinMeta();

		ThriftGenericEntityDao joinEntityDao = context.findEntityDao(joinMeta
				.getTableName());

		List<Pair<Composite, String>> columns = fetchColumns(context,
				propertyMeta);

		Map<Object, Object> map = new HashMap<Object, Object>();
		Map<Object, Object> partialMap = new HashMap<Object, Object>();

		List<Object> joinIds = new ArrayList<Object>();

		for (Pair<Composite, String> pair : columns) {
			Object key = propertyMeta.forceDecodeFromJSON(
					pair.left.get(2, STRING_SRZ), propertyMeta.getKeyClass());
			Object joinId = propertyMeta.forceDecodeFromJSON(pair.right);
			partialMap.put(key, joinId);
			joinIds.add(joinId);
		}

		if (joinIds.size() > 0) {
			log.trace(
					"Loading join entities of class {} having primary keys {}",
					propertyMeta.getValueClass().getCanonicalName(), joinIds);

			Map<Object, Object> entitiesMap = joinHelper.loadJoinEntities(
					propertyMeta.getValueClass(), joinIds, joinMeta,
					joinEntityDao);

			for (Entry<Object, Object> entry : partialMap.entrySet()) {
				map.put(entry.getKey(), entitiesMap.get(entry.getValue()));
			}
		}
		return map;
	}

	private void fillCollectionWithJoinEntities(PropertyMeta propertyMeta,
			EntityMeta joinMeta, List<Object> joinIds,
			ThriftGenericEntityDao joinEntityDao,
			Collection<Object> joinEntities) {
		if (joinIds.size() > 0) {
			Map<Object, Object> entitiesMap = joinHelper.loadJoinEntities(
					propertyMeta.getValueClass(), joinIds, joinMeta,
					joinEntityDao);

			for (Object joinId : joinIds) {
				joinEntities.add(entitiesMap.get(joinId));
			}
		}
	}
}
