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
package info.archinnov.achilles.helper;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.type.KeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityMapper {

	private static final Logger log = LoggerFactory
			.getLogger(EntityMapper.class);

	protected ReflectionInvoker invoker = new ReflectionInvoker();

	protected void addToList(Map<String, List<Object>> listProperties,
			PropertyMeta listMeta, Object value) {
		String propertyName = listMeta.getPropertyName();
		List<Object> list = null;
		if (!listProperties.containsKey(propertyName)) {
			list = new ArrayList<Object>();
			listProperties.put(propertyName, list);
		} else {
			list = listProperties.get(propertyName);
		}
		list.add(value);
	}

	protected void addToSet(Map<String, Set<Object>> setProperties,
			PropertyMeta setMeta, Object value) {
		String propertyName = setMeta.getPropertyName();

		Set<Object> set = null;
		if (!setProperties.containsKey(propertyName)) {
			set = new HashSet<Object>();
			setProperties.put(propertyName, set);
		} else {
			set = setProperties.get(propertyName);
		}
		set.add(value);
	}

	protected void addToMap(Map<String, Map<Object, Object>> mapProperties,
			PropertyMeta mapMeta, KeyValue<?, ?> keyValue) {
		String propertyName = mapMeta.getPropertyName();

		Map<Object, Object> map = null;
		if (!mapProperties.containsKey(propertyName)) {
			map = new HashMap<Object, Object>();
			mapProperties.put(propertyName, map);
		} else {
			map = mapProperties.get(propertyName);
		}
		map.put(keyValue.getKey(), mapMeta.castValue(keyValue.getValue()));
	}

	public <T, ID> void setIdToEntity(ID key, PropertyMeta idMeta, T entity) {
		log.trace("Set primary key value {} to entity {} ", key, entity);

		try {
			invoker.setValueToField(entity, idMeta.getSetter(), key);
		} catch (Exception e) {
			throw new AchillesException("Cannot set value '" + key
					+ "' to entity " + entity, e);
		}
	}

	public <T, ID> void setSimplePropertyToEntity(String value,
			PropertyMeta propertyMeta, T entity) {
		log.trace("Set simple property {} to entity {} ",
				propertyMeta.getPropertyName(), entity);

		try {
			invoker.setValueToField(entity, propertyMeta.getSetter(),
					propertyMeta.getValueFromString(value));
		} catch (Exception e) {
			throw new AchillesException("Cannot set value '" + value
					+ "' to entity " + entity, e);
		}
	}

	public void setListPropertyToEntity(List<?> list, PropertyMeta listMeta,
			Object entity) {
		log.trace("Set list property {} to entity {} ",
				listMeta.getPropertyName(), entity);

		try {
			invoker.setValueToField(entity, listMeta.getSetter(), list);
		} catch (Exception e) {
			throw new AchillesException("Cannot set value '" + list
					+ "' to entity " + entity, e);
		}
	}

	public void setSetPropertyToEntity(Set<?> set, PropertyMeta setMeta,
			Object entity) {
		log.trace("Set set property {} to entity {} ",
				setMeta.getPropertyName(), entity);

		try {
			invoker.setValueToField(entity, setMeta.getSetter(), set);
		} catch (Exception e) {
			throw new AchillesException("Cannot set value '" + set
					+ "' to entity " + entity, e);
		}
	}

	public void setMapPropertyToEntity(Map<?, ?> map, PropertyMeta mapMeta,
			Object entity) {
		log.trace("Set map property {} to entity {} ",
				mapMeta.getPropertyName(), entity);

		try {
			invoker.setValueToField(entity, mapMeta.getSetter(), map);
		} catch (Exception e) {
			throw new AchillesException("Cannot set value '" + map
					+ "' to entity " + entity, e);
		}
	}
}
