/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.proxy.wrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			getDirtyChecker().removeAllElements();
		}
		this.target.clear();

	}

	@Override
	public boolean containsKey(Object key) {
		return this.target.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this.target.containsValue(proxifier.removeProxy(value));
	}

	@Override
	public Set<Entry<Object, Object>> entrySet() {
		return new HashSet<>(this.target.entrySet());
	}

	@Override
	public Object get(Object key) {
        if(log.isTraceEnabled()) {
            log.trace("Return value having key{} for map property {} of entity class {}", key,
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        }
		return this.target.get(key);
	}

	@Override
	public boolean isEmpty() {
		return this.target.isEmpty();
	}

	@Override
	public Set<Object> keySet() {
		return new HashSet<>(this.target.keySet());
	}

	@Override
	public Object put(Object key, Object value) {
		log.trace("Mark map property {} of entity class {} dirty upon new value {} addition for key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), value, key);

        final Object element = proxifier.removeProxy(value);
        Object result = this.target.put(key, element);
        HashMap<Object, Object> entries = new HashMap<>();
        entries.put(key,value);
        getDirtyChecker().addElements(entries);
		return result;
	}

	@Override
	public void putAll(Map<?, ?> map) {
		Map<Object, Object> encodedMap = new HashMap<Object, Object>();
		for (Entry<?, ?> entry : map.entrySet()) {
            final Object element = proxifier.removeProxy(entry.getValue());
            final Object key = entry.getKey();
            encodedMap.put(key, element);
		}
        if(encodedMap.size()>0) {
            getDirtyChecker().addElements(encodedMap);
        }

		log.trace("Mark encodedMap property {} of entity class {} dirty upon new key/value pairs addition",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		this.target.putAll(encodedMap);
	}

	@Override
	public Object remove(Object key) {
		Object element = proxifier.removeProxy(key);
		if (this.target.containsKey(element)) {
			log.trace("Mark map property {} of entity class {} dirty upon removal of value havo,g key {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), key);
            getDirtyChecker().removeMapEntry(key);
		}
		return this.target.remove(element);
	}

	@Override
	public int size() {
		return this.target.size();
	}

	@Override
	public Collection<Object> values() {
		return new ArrayList<>(this.target.values());
	}

	public Map<Object, Object> getTarget() {
		return target;
	}

    @Override
    protected DirtyChecker getDirtyChecker() {
        if(!dirtyMap.containsKey(setter)) {
            dirtyMap.put(setter,new DirtyChecker(propertyMeta));
        }
        return dirtyMap.get(setter);
    }
}
