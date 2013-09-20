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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityMapper {

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
			PropertyMeta mapMeta, Object decodedKey, Object decodedValue) {
		String propertyName = mapMeta.getPropertyName();

		Map<Object, Object> map = null;
		if (!mapProperties.containsKey(propertyName)) {
			map = new HashMap<Object, Object>();
			mapProperties.put(propertyName, map);
		} else {
			map = mapProperties.get(propertyName);
		}
		map.put(decodedKey, decodedValue);
	}
}
