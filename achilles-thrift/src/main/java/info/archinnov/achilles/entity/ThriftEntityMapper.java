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
package info.archinnov.achilles.entity;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.helper.EntityMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftEntityMapper extends EntityMapper {
	private static final Logger log = LoggerFactory
			.getLogger(ThriftEntityMapper.class);

	public void setEagerPropertiesToEntity(Object key,
			List<Pair<Composite, String>> columns, EntityMeta entityMeta,
			Object entity) {
		log.trace("Set eager properties to entity {} ",
				entityMeta.getClassName());

		Map<String, List<Object>> listProperties = new HashMap<String, List<Object>>();
		Map<String, Set<Object>> setProperties = new HashMap<String, Set<Object>>();
		Map<String, Map<Object, Object>> mapProperties = new HashMap<String, Map<Object, Object>>();

		setIdToEntity(key, entityMeta.getIdMeta(), entity);

		Map<String, PropertyMeta> propertyMetas = entityMeta.getPropertyMetas();

		for (Pair<Composite, String> pair : columns) {
			String propertyName = pair.left.get(1, STRING_SRZ);

			PropertyMeta propertyMeta = propertyMetas.get(propertyName);

			if (propertyMeta != null) {

				switch (propertyMeta.type()) {
				case SIMPLE:
					setSimplePropertyToEntity(pair.right, propertyMeta, entity);
					break;
				case LIST:
					addToList(listProperties, propertyMeta,
							propertyMeta.getValueFromString(pair.right));
					break;
				case SET:
					addToSet(setProperties, propertyMeta,
							propertyMeta.getValueFromString(pair.right));
					break;
				case MAP:
					addToMap(mapProperties, propertyMeta,
							propertyMeta.getKeyValueFromString(pair.right));
					break;
				default:
					log.debug("Property {} is lazy or of proxy type, do not set to entity now");
					break;
				}
			} else {
				log.warn("No field mapping for property {}", propertyName);
			}
		}

		setMultiValuesProperties(entity, listProperties, setProperties,
				mapProperties, propertyMetas);
	}

	public <T> T initClusteredEntity(Class<T> entityClass, PropertyMeta idMeta,
			Object embeddedId) {
		T clusteredEntity = null;
		clusteredEntity = invoker.instanciate(entityClass);
		invoker.setValueToField(clusteredEntity, idMeta.getSetter(), embeddedId);
		return clusteredEntity;
	}

	public <T> T createClusteredEntityWithValue(Class<T> entityClass,
			PropertyMeta idMeta, PropertyMeta pm, Object embeddedId,
			Object clusteredValue) {
		T clusteredEntity = null;
		clusteredEntity = invoker.instanciate(entityClass);
		invoker.setValueToField(clusteredEntity, idMeta.getSetter(), embeddedId);
		invoker.setValueToField(clusteredEntity, pm.getSetter(), clusteredValue);

		return clusteredEntity;
	}

	private void setMultiValuesProperties(Object entity,
			Map<String, List<Object>> listProperties,
			Map<String, Set<Object>> setProperties,
			Map<String, Map<Object, Object>> mapProperties,
			Map<String, PropertyMeta> propertyMetas) {
		for (Entry<String, List<Object>> entry : listProperties.entrySet()) {
			setListPropertyToEntity(entry.getValue(),
					propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Set<Object>> entry : setProperties.entrySet()) {
			setSetPropertyToEntity(entry.getValue(),
					propertyMetas.get(entry.getKey()), entity);
		}

		for (Entry<String, Map<Object, Object>> entry : mapProperties
				.entrySet()) {
			setMapPropertyToEntity(entry.getValue(),
					propertyMetas.get(entry.getKey()), entity);
		}
	}
}
