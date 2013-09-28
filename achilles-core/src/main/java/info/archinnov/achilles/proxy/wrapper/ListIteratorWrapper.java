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

import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListIteratorWrapper extends AbstractWrapper implements
		ListIterator<Object> {
	private static final Logger log = LoggerFactory
			.getLogger(ListIteratorWrapper.class);

	private ListIterator<Object> target;

	public ListIteratorWrapper(ListIterator<Object> target) {
		this.target = target;
	}

	@Override
	public void add(Object e) {
		log.trace(
				"Mark list property {} of entity class {} dirty upon element addition",
				propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		this.target.add(proxifier.unwrap(e));
		this.markDirty();
	}

	@Override
	public boolean hasNext() {
		return this.target.hasNext();
	}

	@Override
	public boolean hasPrevious() {
		return this.target.hasPrevious();
	}

	@Override
	public Object next() {
		log.trace(
				"Return next element from list property {} of entity class {}",
				propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		return this.target.next();
	}

	@Override
	public int nextIndex() {
		return this.target.nextIndex();
	}

	@Override
	public Object previous() {
		log.trace(
				"Return previous element from list property {} of entity class {}",
				propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		return this.target.previous();
	}

	@Override
	public int previousIndex() {
		return this.target.previousIndex();
	}

	@Override
	public void remove() {
		log.trace(
				"Mark list property {} of entity class {} dirty upon element removal",
				propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		this.target.remove();
		this.markDirty();
	}

	@Override
	public void set(Object e) {
		log.trace(
				"Mark list property {} of entity class {} dirty upon element set at current position",
				propertyMeta.getPropertyName(),
				propertyMeta.getEntityClassName());
		this.target.set(proxifier.unwrap(e));
		this.markDirty();
	}

}
