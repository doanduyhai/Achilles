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

package info.archinnov.achilles.internal.proxy.dirtycheck;

import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.addAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.appendAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.discardAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.prependAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.put;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.removeAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.setIdx;
import static java.util.Map.Entry;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

public class DirtyCheckChangeSet {

    private final CollectionAndMapChangeType changeType;
    private final PropertyMeta propertyMeta;
    protected List<Object> listChanges = new ArrayList<>();
    protected ElementAtIndex listChangeAtIndex = null;
    protected Set<Object> setChanges = new HashSet<>();
    protected Map<Object,Object> mapChanges = new HashMap<>();

    public DirtyCheckChangeSet(PropertyMeta propertyMeta,CollectionAndMapChangeType changeType) {
        this.propertyMeta = propertyMeta;
        this.changeType = changeType;
    }

    public List<Object> getEncodedListChanges() {
        if(isNotEmpty(listChanges)) {
            return propertyMeta.encode(listChanges);
        }
        return listChanges;
    }

    public ElementAtIndex getEncodedListChangeAtIndex() {
        if(listChangeAtIndex != null && listChangeAtIndex.getElement() != null) {
            Object encodedElement = propertyMeta.encode(listChangeAtIndex.getElement());
            return new ElementAtIndex(listChangeAtIndex.getIndex(),encodedElement);
        }
        return listChangeAtIndex;
    }

    public Set<Object> getEncodedSetChanges() {
        if(isNotEmpty(setChanges)) {
            return propertyMeta.encode(setChanges);
        }
        return setChanges;
    }

    public Map<Object, Object> getEncodedMapChanges() {
        if(MapUtils.isNotEmpty(mapChanges)) {
            Map<Object,Object> encodedMapChanges = new HashMap<>();
            for(Entry<Object,Object> entry : mapChanges.entrySet()) {
                Object encodedKey = propertyMeta.encodeKey(entry.getKey());
                Object encodedValue = propertyMeta.encode(entry.getValue());
                encodedMapChanges.put(encodedKey,encodedValue);
            }
            return encodedMapChanges;
        }
        return mapChanges;
    }


    public CollectionAndMapChangeType getChangeType() {
        return changeType;
    }

    public PropertyMeta getPropertyMeta() {
        return propertyMeta;
    }

    public Object[] generateUpdateForAddedElements(Update.Assignments with, boolean preparedStatement) {
        Set<Object> encodedElements = null;

        String propertyName = propertyMeta.getPropertyName();
        if(preparedStatement) {
            with.and(addAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedSetChanges();
            with.and(addAll(propertyName, encodedElements));
        }
        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForRemovedElements(Update.Assignments with, boolean preparedStatement) {
        Set<Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(removeAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedSetChanges();
            with.and(removeAll(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForAppendedElements(Update.Assignments with, boolean preparedStatement) {
        List<Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(appendAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            with.and(appendAll(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForPrependedElements(Update.Assignments with, boolean preparedStatement) {
        List<Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(prependAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            with.and(prependAll(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForRemoveListElements(Update.Assignments with, boolean preparedStatement) {
        List<Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(discardAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            with.and(discardAll(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForSetAtIndexElement(Update.Assignments with) {
        final ElementAtIndex elementAtIndex = getEncodedListChangeAtIndex();
        final int index = elementAtIndex.getIndex();
        final Object encoded = elementAtIndex.getElement();
        String propertyName = propertyMeta.getPropertyName();
        with.and(setIdx(propertyName, index, encoded));
        return new Object[]{index,encoded};
    }

    public Object[] generateUpdateForRemovedAtIndexElement(Update.Assignments with) {
        String propertyName = propertyMeta.getPropertyName();
        with.and(setIdx(propertyName, listChangeAtIndex.getIndex(), null));
        return new Object[]{listChangeAtIndex.getIndex(),null};
    }

    public Object[] generateUpdateForAddedEntries(Update.Assignments with, boolean preparedStatement) {
        Map<Object, Object> encodedEntries = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(putAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedEntries = getEncodedMapChanges();
            with.and(putAll(propertyName, encodedEntries));
        }

        return new Object[]{encodedEntries};
    }

    public Object[] generateUpdateForRemovedKey(Update.Assignments with, boolean preparedStatement) {
        String propertyName = propertyMeta.getPropertyName();
        Object encodedKey = propertyMeta.encodeKey(mapChanges.keySet().iterator().next());

        if(preparedStatement) {
            with.and(put(propertyName,bindMarker("key"),bindMarker("nullValue")));
        } else {
            with.and(put(propertyName, encodedKey,null));
        }

        return new Object[]{encodedKey,null};
    }

    public Object[] generateUpdateForRemoveAll(Update.Assignments with, boolean preparedStatement) {
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(set(propertyName,bindMarker(propertyName)));
        } else {
            with.and(set(propertyName, null));
        }

        return new Object[]{null};
    }

    public Object[] generateUpdateForAssignValueToList(Update.Assignments with, boolean preparedStatement) {
        List<Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(set(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            with.and(set(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForAssignValueToSet(Update.Assignments with, boolean preparedStatement) {
        Set<Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(set(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedSetChanges();
            with.and(set(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }

    public Object[] generateUpdateForAssignValueToMap(Update.Assignments with, boolean preparedStatement) {
        Map<Object,Object> encodedElements = null;
        String propertyName = propertyMeta.getPropertyName();

        if(preparedStatement) {
            with.and(set(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedMapChanges();
            with.and(set(propertyName, encodedElements));
        }

        return new Object[]{encodedElements};
    }


    void setListChanges(List<Object> listChanges) {
        this.listChanges = listChanges;
    }

    void setListChangeAtIndex(ElementAtIndex listChangeAtIndex) {
        this.listChangeAtIndex = listChangeAtIndex;
    }

    void setSetChanges(Set<Object> setChanges) {
        this.setChanges = setChanges;
    }

    void setMapChanges(Map<Object, Object> mapChanges) {
        this.mapChanges = mapChanges;
    }

    public List<Object> getRawListChanges() {
        return listChanges;
    }

    public Set<Object> getRawSetChanges() {
        return setChanges;
    }

    public Map<Object, Object> getRawMapChanges() {
        return mapChanges;
    }

    public static class ElementAtIndex {

        private int index;
        private Object element;

        public ElementAtIndex(int index, Object element) {
            this.index = index;
            this.element = element;
        }

        public int getIndex() {
            return index;
        }

        public Object getElement() {
            return element;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ElementAtIndex that = (ElementAtIndex) o;

            return index == that.index &&
                    !(element != null ? !element.equals(that.element) : that.element != null);

        }

        @Override
        public int hashCode() {
            int result = index;
            result = 31 * result + (element != null ? element.hashCode() : 0);
            return result;
        }
    }
}
