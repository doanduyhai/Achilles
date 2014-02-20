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
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListWrapper extends CollectionWrapper implements List<Object> {
	private static final Logger log = LoggerFactory.getLogger(ListWrapper.class);

	public ListWrapper(List<Object> target) {
		super(target);
	}

	@Override
	public void add(int index, Object arg1) {
		log.trace("Mark list property {} of entity class {} dirty upon element addition at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);
		((List<Object>) super.target).add(index, proxifier.removeProxy(arg1));
		super.markDirty();
	}

	@Override
	public boolean addAll(int arg0, Collection<? extends Object> arg1) {
		boolean result = ((List<Object>) super.target).addAll(arg0, proxifier.removeProxy(arg1));
		if (result) {
			log.trace("Mark list property {} of entity class {} dirty upon elements addition",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			super.markDirty();
		}
		return result;
	}

	@Override
	public Object get(int index) {
		log.trace("Return element at index {} for list property {} of entity class {}", index,
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		return ((List<Object>) super.target).get(index);
	}

	@Override
	public int indexOf(Object arg0) {
		return ((List<Object>) super.target).indexOf(arg0);
	}

	@Override
	public int lastIndexOf(Object arg0) {
		return ((List<Object>) super.target).lastIndexOf(arg0);
	}

	@Override
	public ListIterator<Object> listIterator() {
		return new ArrayList<>(super.target).listIterator();
	}

	@Override
	public ListIterator<Object> listIterator(int index) {
		return new ArrayList<>(super.target).listIterator(index);
	}

	@Override
	public Object remove(int index) {
		log.trace("Mark list property {} of entity class {} dirty upon element removal at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);

		Object result = ((List<Object>) super.target).remove(index);
		super.markDirty();
		return result;
	}

	@Override
	public Object set(int index, Object arg1) {
		log.trace("Mark list property {} of entity class {} dirty upon element set at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		Object result = ((List<Object>) super.target).set(index, proxifier.removeProxy(arg1));
		super.markDirty();
		return result;
	}

	@Override
	public List<Object> subList(int from, int to) {
		return new ArrayList<>(super.target).subList(from, to);
	}

	@Override
	public List<Object> getTarget() {
		return ((List<Object>) super.target);
	}

}
