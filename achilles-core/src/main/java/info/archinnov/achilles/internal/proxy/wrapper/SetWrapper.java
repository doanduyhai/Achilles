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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Sets.SetView;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.collect.Sets.newHashSet;

public class SetWrapper extends AbstractWrapper implements Set<Object> {
	private static final Logger log = LoggerFactory.getLogger(SetWrapper.class);

	private Set<Object> target;

	public SetWrapper(Set<Object> target) {
		this.target = target;
	}

	@Override
	public boolean add(Object arg0) {
		log.trace("Mark set property {} of entity class {} dirty upon element addition",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        final Object element = proxifier.removeProxy(arg0);
        boolean added = target.add(element);
        if(added) {
            getDirtyChecker().addElements(newHashSet(arg0));
        }
		return added;
	}

	@Override
	public boolean addAll(Collection<?> arg0) {
        final Collection<Object> elements = (Collection<Object>) proxifier.removeProxy(arg0);
        boolean added = target.addAll(elements);
		if (added) {
			log.trace("Mark set property {} of entity class {} dirty upon elements addition",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            getDirtyChecker().addElements(newHashSet(elements));
		}
		return added;
	}

	@Override
	public void clear() {
		if (this.target.size() > 0) {
			log.trace("Mark set property {} of entity class {} dirty upon clearance",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.getDirtyChecker().removeAllElements();
		}
		this.target.clear();
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
	public Iterator<Object> iterator() {
		log.trace("Build iterator wrapper for set property {} of entity class {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		return new ArrayList<>(this.target).iterator();
	}

	@Override
	public boolean remove(Object arg0) {
        final Object element = proxifier.removeProxy(arg0);
        boolean result = this.target.remove(element);
		if (result) {
			log.trace("Mark set property {} of entity class {} dirty upon element removal",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			this.getDirtyChecker().removeElements(newHashSet(element));
		}
		return result;
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
        final Collection<Object> elements = (Collection<Object>) proxifier.removeProxy(arg0);
        boolean result = this.target.removeAll(elements);
		if (result) {
			log.trace("Mark set property {} of entity class {} dirty upon elements removal",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.getDirtyChecker().removeElements(newHashSet(elements));
		}
		return result;
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
        final Collection<Object> elements = (Collection<Object>) proxifier.removeProxy(arg0);
        final HashSet<Object> originalSet = new HashSet<>(this.target);
        boolean result = this.target.retainAll(elements);
        if (result) {
            log.trace("Mark set property {} of entity class {} dirty upon elements retentions",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            final SetView<Object> intersection = intersection(originalSet, new HashSet<>(arg0));
            final SetView<Object> difference = difference(originalSet, intersection);
            if(difference.size()>0) {
                this.getDirtyChecker().removeElements(difference);
            }
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

	public Collection<Object> getTarget() {
		return this.target;
	}

    @Override
    protected DirtyChecker getDirtyChecker() {
        if(!dirtyMap.containsKey(setter)) {
            dirtyMap.put(setter,new DirtyChecker(propertyMeta));
        }
        return dirtyMap.get(setter);
    }
}
