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

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class UpdateMapWrapper extends AbstractWrapper implements Map<Object, Object> {
	private static final Logger log = LoggerFactory.getLogger(UpdateMapWrapper.class);

    /**
     * Clear data without read-before-write
     */
	@Override
	public void clear() {
        log.trace("Mark map property {} of entity class {} dirty upon all elements clearance",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        getDirtyChecker().removeAllElements();
	}

	@Override
	public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public Set<Entry<Object, Object>> entrySet() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public Object get(Object key) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public boolean isEmpty() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public Set<Object> keySet() {
		throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

    /**
     * For update without read-before-write behavior, <strong>this method always returns Optional.absent()</strong>
     * @param key for the element to be put
     * @param element to be put for key
     * @return Optional.absent()
     */
	@Override
	public Object put(Object key, Object element) {
		log.trace("Mark map property {} of entity class {} dirty upon new value {} addition for key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), element, key);

        final Object rawElement = proxifier.removeProxy(element);
        HashMap<Object, Object> entries = new HashMap<>();
        entries.put(key,rawElement);
        getDirtyChecker().addElements(entries);
		return Optional.absent();
	}

    /**
     * For update without read-before-write behavior
     * @param map to be put
     */
	@Override
	public void putAll(Map<?, ?> map) {
        log.trace("Mark encodedMap property {} of entity class {} dirty upon new key/value pairs addition",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        Map<Object, Object> encodedMap = new HashMap<>();
        for (Entry<?, ?> entry : map.entrySet()) {
            final Object element = proxifier.removeProxy(entry.getValue());
            final Object key = proxifier.removeProxy(entry.getKey());
            encodedMap.put(key, element);
        }
        if(encodedMap.size()>0) {
            getDirtyChecker().addElements(encodedMap);
        }

	}

    /**
     * For update without read-before-write behavior, <strong>this method always returns Optional.absent()</strong>
     * @param key for the element to be removed
     * @return Optional.absent()
     */
	@Override
	public Object remove(Object key) {
        log.trace("Mark map property {} of entity class {} dirty upon removal of value havo,g key {}",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), key);
        Object element = proxifier.removeProxy(key);
        getDirtyChecker().removeMapEntry(element);
        return Optional.absent();
    }

	@Override
	public int size() {
		throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public Collection<Object> values() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	public Map<Object, Object> getTarget() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}
}
