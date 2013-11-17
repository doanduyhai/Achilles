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
package info.archinnov.achilles.proxy.wrapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.proxy.wrapper.builder.EntrySetWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.KeySetWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.ValueCollectionWrapperBuilder;

public class MapWrapper extends AbstractWrapper implements Map<Object, Object> {
	private static final Logger log = LoggerFactory.getLogger(MapWrapper.class);

	private final Map<Object, Object> target;

	public MapWrapper(Map<Object, Object> target) {
		this.target = target;
	}

	@Override
	public void clear() {
		if (this.target.size() > 0) {
			log.trace("Mark map property {} of entity class {} dirty upon all elements clearance",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			this.markDirty();
		}
		this.target.clear();

	}

	@Override
	public boolean containsKey(Object key) {
		return this.target.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this.target.containsValue(proxifier.unwrap(value));
	}

	@Override
	public Set<Entry<Object, Object>> entrySet() {
		Set<Entry<Object, Object>> targetEntrySet = this.target.entrySet();
		if (targetEntrySet.size() > 0) {
			log.trace("Build map entry wrapper for map property {} of entity class {}", propertyMeta.getPropertyName(),
					propertyMeta.getEntityClassName());

			EntrySetWrapper wrapperSet = EntrySetWrapperBuilder.builder(context, targetEntrySet).dirtyMap(dirtyMap)
					.setter(setter).propertyMeta(propertyMeta).proxifier(proxifier).build();
			targetEntrySet = wrapperSet;
		}
		return targetEntrySet;
	}

	@Override
	public Object get(Object key) {
		log.trace("Return value having key{} for map property {} of entity class {}", key,
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return this.target.get(key);
	}

	@Override
	public boolean isEmpty() {
		return this.target.isEmpty();
	}

	@Override
	public Set<Object> keySet() {
		Set<Object> keySet = this.target.keySet();
		if (keySet.size() > 0) {
			log.trace("Build key set wrapper for map property {} of entity class {}", propertyMeta.getPropertyName(),
					propertyMeta.getEntityClassName());

			KeySetWrapper keySetWrapper = KeySetWrapperBuilder.builder(context, keySet).dirtyMap(dirtyMap)
					.setter(setter).propertyMeta(propertyMeta).proxifier(proxifier).build();
			keySet = keySetWrapper;
		}
		return keySet;
	}

	@Override
	public Object put(Object key, Object value) {
		log.trace("Mark map property {} of entity class {} dirty upon new value {} addition for key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), value, key);

		Object result = this.target.put(key, proxifier.unwrap(value));
		this.markDirty();
		return result;
	}

	@Override
	public void putAll(Map<?, ?> m) {
		Map<Object, Object> map = new HashMap<Object, Object>();
		for (Entry<?, ?> entry : m.entrySet()) {
			map.put(entry.getKey(), proxifier.unwrap(entry.getValue()));
		}

		log.trace("Mark map property {} of entity class {} dirty upon new key/value pairs addition",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		this.target.putAll(map);
		this.markDirty();
	}

	@Override
	public Object remove(Object key) {
		Object unproxy = proxifier.unwrap(key);
		if (this.target.containsKey(unproxy)) {
			log.trace("Mark map property {} of entity class {} dirty upon removal of value havo,g key {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), key);
			this.markDirty();
		}
		return this.target.remove(unproxy);
	}

	@Override
	public int size() {
		return this.target.size();
	}

	@Override
	public Collection<Object> values() {
		Collection<Object> values = this.target.values();

		if (values.size() > 0) {
			log.trace("Build values collection wrapper for map property {} of entity class {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

			ValueCollectionWrapper collectionWrapper = ValueCollectionWrapperBuilder
					//
					.builder(context, values).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta)
					.proxifier(proxifier).build();
			values = collectionWrapper;
		}
		return values;
	}

	public Map<Object, Object> getTarget() {
		return target;
	}
}
