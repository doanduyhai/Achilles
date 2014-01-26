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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.proxy.wrapper.builder.EntryIteratorWrapperBuilder;

public class EntrySetWrapper extends AbstractWrapper implements Set<Entry<Object, Object>> {
	private static final Logger log = LoggerFactory.getLogger(EntrySetWrapper.class);

	private Set<Entry<Object, Object>> target;

	public EntrySetWrapper(Set<Entry<Object, Object>> target) {
		this.target = target;
	}

	@Override
	public boolean add(Entry<Object, Object> arg0) {
		throw new UnsupportedOperationException("This method is not supported for an Entry set");
	}

	@Override
	public boolean addAll(Collection<? extends Entry<Object, Object>> arg0) {
		throw new UnsupportedOperationException("This method is not supported for an Entry set");
	}

	@Override
	public void clear() {
		log.trace("Mark dirty for property {} of entity class {} upon entry set clearance",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		this.target.clear();
		this.markDirty();
	}

	@Override
	public boolean contains(Object arg0) {
		return this.target.contains(proxifier.removeProxy(arg0));
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		return this.target.containsAll(proxifier.removeProxy(arg0));
	}

	@Override
	public boolean isEmpty() {
		return this.target.isEmpty();
	}

	@Override
	public Iterator<Entry<Object, Object>> iterator() {
		log.trace("Build iterator wrapper for entry set of property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return EntryIteratorWrapperBuilder
				.builder(context, this.target.iterator()).dirtyMap(dirtyMap).setter(setter).propertyMeta(propertyMeta)
				.build();
	}

	@Override
	public boolean remove(Object arg0) {
		boolean result = false;
		result = this.target.remove(proxifier.removeProxy(arg0));
		if (result) {
			log.trace("Mark dirty for property {} of entity class {} upon entry removal",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			this.markDirty();
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		boolean result = false;
		result = this.target.removeAll(proxifier.removeProxy(arg0));
		if (result) {
			log.trace("Mark dirty for property {} of entity class {} upon all entries removal",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			this.markDirty();
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		boolean result = false;
		result = this.target.retainAll(proxifier.removeProxy(arg0));
		if (result) {
			log.trace("Mark dirty for property {} of entity class {} upon entries retaining",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			this.markDirty();
		}
		return result;
	}

	@Override
	public int size() {
		return this.target.size();
	}

	@Override
	public Object[] toArray() {
		return this.target.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		return this.target.toArray(arg0);
	}

}
