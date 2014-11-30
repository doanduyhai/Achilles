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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.commons.collections.ListUtils.intersection;

public class ListWrapper extends AbstractWrapper implements List<Object> {
	private static final Logger log = LoggerFactory.getLogger(ListWrapper.class);

    private List<Object> target;

    public ListWrapper(List<Object> target) {
        this.target = target;
    }

    @Override
    public boolean add(Object arg0) {
        log.trace("Mark list property {} of entity class {} dirty upon element addition",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        final Object element = proxifier.removeProxy(arg0);
        getDirtyChecker().appendListElements(asList(arg0));

        return target.add(element);
    }

    @Override
    public boolean addAll(Collection<?> arg0) {
        final Collection<Object> elements = (Collection<Object>) proxifier.removeProxy(arg0);
        boolean added = target.addAll(elements);
        if (added) {
            log.trace("Mark list property {} of entity class {} dirty upon elements addition",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            getDirtyChecker().appendListElements(new ArrayList<>(elements));
        }
        return added;
    }


	@Override
	public void add(int index, Object arg1) {
		log.trace("Mark list property {} of entity class {} dirty upon element addition at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);
        if(index != 0) {
            throw new UnsupportedOperationException("Append, Prepend, Remove, RemoveAll and SetValueAtIndex are the only supported operations for CQL lists");
        }
        final Object element = proxifier.removeProxy(arg1);
        target.add(index, element);
		getDirtyChecker().prependListElements(asList(element));
	}

	@Override
	public boolean addAll(int index, Collection<?> arg1) {
        if(index != 0) {
            throw new UnsupportedOperationException("Append, Prepend, Remove, RemoveAll and SetValueAtIndex are the only supported operations for CQL lists");
        }
        final Collection<Object> elements = (Collection<Object>) proxifier.removeProxy(arg1);
        boolean result = target.addAll(index, elements);
		if (result) {
			log.trace("Mark list property {} of entity class {} dirty upon elements addition",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            getDirtyChecker().prependListElements(new ArrayList<>(elements));
		}
		return result;
	}


    @Override
    public void clear() {
        if (target.size() > 0) {
            log.trace("Mark list property {} of entity class {} dirty upon clearance",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.getDirtyChecker().removeAllElements();
        }
        target.clear();
    }

	@Override
	public Object get(int index) {
        if(log.isTraceEnabled()) {
            log.trace("Return element at index {} for list property {} of entity class {}", index,
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        }
		return target.get(index);
	}


	@Override
	public int indexOf(Object arg0) {
		return target.indexOf(proxifier.removeProxy(arg0));
	}

	@Override
	public int lastIndexOf(Object arg0) {
		return target.lastIndexOf(proxifier.removeProxy(arg0));
	}

	@Override
	public ListIterator<Object> listIterator() {
		return new ArrayList<>(target).listIterator();
	}

	@Override
	public ListIterator<Object> listIterator(int index) {
		return new ArrayList<>(target).listIterator(index);
	}

    @Override
    public boolean contains(Object arg0) {
        return target.contains(proxifier.removeProxy(arg0));
    }

    @Override
    public boolean containsAll(Collection<?> arg0) {
        return target.containsAll(proxifier.removeProxy(arg0));
    }

    @Override
    public boolean isEmpty() {
        return target.isEmpty();
    }

    @Override
    public Iterator<Object> iterator() {
        log.trace("Build iterator wrapper for list property {} of entity class {}",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        return new ArrayList<>(target).iterator();
    }

    @Override
    public boolean remove(Object arg0) {
        final Object element = proxifier.removeProxy(arg0);
        boolean result = this.target.remove(element);
        if (result) {
            log.trace("Mark list property {} of entity class {} dirty upon element removal",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.getDirtyChecker().removeElements(asList(element));
        }
        return result;
    }

    @Override
    public boolean removeAll(Collection<?> arg0) {
        final Collection<Object> elements = (Collection<Object>) proxifier.removeProxy(arg0);
        boolean result = this.target.removeAll(elements);
        if (result) {
            log.trace("Mark list property {} of entity class {} dirty upon elements removal",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.getDirtyChecker().removeElements(new ArrayList<>(elements));
        }
        return result;
    }

    @Override
    public boolean retainAll(Collection<?> arg0) {
        final ArrayList<Object> argumentList = new ArrayList<>(proxifier.removeProxy(arg0));
        final ArrayList<Object> originalList = new ArrayList<>(target);
        boolean result = this.target.retainAll(argumentList);
        if (result) {
            log.trace("Mark list property {} of entity class {} dirty upon elements removal",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            final List intersectionList = intersection(originalList, argumentList);
            final List toBeRemoved = ListUtils.subtract(originalList, intersectionList);
            this.getDirtyChecker().removeElements(toBeRemoved);
        }
        return result;
    }

    @Override
    public int size() {
        return target.size();
    }

    @Override
    public Object[] toArray() {
        return target.toArray();
    }

    @Override
    public <T> T[] toArray(T[] arg0) {
        return target.toArray(arg0);
    }

	@Override
	public Object remove(int index) {
		log.trace("Mark list property {} of entity class {} dirty upon element removal at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), index);

		Object result = target.remove(index);
        getDirtyChecker().removeListElementAtIndex(index);
		return result;
	}

	@Override
	public Object set(int index, Object arg1) {
		log.trace("Mark list property {} of entity class {} dirty upon element set at index {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

        final Object element = proxifier.removeProxy(arg1);
        Object result = target.set(index, element);
		getDirtyChecker().setListElementAtIndex(index, element);
		return result;
	}

	@Override
	public List<Object> subList(int from, int to) {
		return new ArrayList<>(target).subList(from, to);
	}

	public List<Object> getTarget() {
		return target;
	}
}
