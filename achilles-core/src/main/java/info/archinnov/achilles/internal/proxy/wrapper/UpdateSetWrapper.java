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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.collect.Sets.*;

public class UpdateSetWrapper extends AbstractWrapper implements Set<Object> {
	private static final Logger log = LoggerFactory.getLogger(UpdateSetWrapper.class);

    /**
     * For update without read-before-write behavior, <strong>this method always returns true</strong>
     * @param element to be added
     * @return true
     */
	@Override
	public boolean add(Object element) {
		log.trace("Mark set property {} of entity class {} dirty upon element addition",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        final Object rawElement = proxifier.removeProxy(element);
        getDirtyChecker().addElements(newHashSet(rawElement));
		return true;
	}

    /**
     * For update without read-before-write behavior, <strong>this method always returns true</strong>
     * @param elements to be added
     * @return true
     */
	@Override
	public boolean addAll(Collection<?> elements) {
        log.trace("Mark set property {} of entity class {} dirty upon elements addition",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        final Collection<Object> rawElements = (Collection<Object>) proxifier.removeProxy(elements);
        if (!rawElements.isEmpty()) {
            getDirtyChecker().addElements(newHashSet(rawElements));
        }
		return true;
	}

    /**
     * Clear data without read-before-write
     */
	@Override
	public void clear() {
        log.trace("Mark set property {} of entity class {} dirty upon clearance",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        this.getDirtyChecker().removeAllElements();
	}

	@Override
	public boolean contains(Object arg0) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public boolean isEmpty() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public Iterator<Object> iterator() {
		throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

    /**
     * For update without read-before-write behavior, <strong>this method always returns true</strong>
     * @param element to be removed
     * @return true
     */
	@Override
	public boolean remove(Object element) {
        log.trace("Mark set property {} of entity class {} dirty upon element removal",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        final Object rawElement = proxifier.removeProxy(element);
        this.getDirtyChecker().removeElements(newHashSet(rawElement));
		return true;
	}

    /**
     * For update without read-before-write behavior, <strong>this method always returns true</strong>
     * @param elements to be removed
     * @return true
     */
	@Override
	public boolean removeAll(Collection<?> elements) {
        log.trace("Mark set property {} of entity class {} dirty upon elements removal",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        final Collection<Object> rawElements = (Collection<Object>) proxifier.removeProxy(elements);
        this.getDirtyChecker().removeElements(newHashSet(rawElements));
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public int size() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public Object[] toArray() {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
        throw new UnsupportedOperationException("This operation is not available on proxy objects for update. We don't want to read-before-write");
	}
}
