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
import static com.datastax.driver.core.querybuilder.Update.Assignments;
import static java.util.Map.Entry;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.type.Pair;

public class DirtyCheckChangeSet {

    private final CollectionAndMapChangeType changeType;
    private final PropertyMeta propertyMeta;
    protected List<Object> listChanges = new ArrayList<>();
    protected ElementAtIndex listChangeAtIndex = null;
    protected Set<Object> setChanges = new HashSet<>();
    protected Map<Object, Object> mapChanges = new HashMap<>();

    public DirtyCheckChangeSet(PropertyMeta propertyMeta, CollectionAndMapChangeType changeType) {
        this.propertyMeta = propertyMeta;
        this.changeType = changeType;
    }

    public List<Object> getEncodedListChanges() {
        if (isNotEmpty(listChanges)) {
            return propertyMeta.encode(listChanges);
        }
        return listChanges;
    }

    public ElementAtIndex getEncodedListChangeAtIndex() {
        if (listChangeAtIndex != null && listChangeAtIndex.getElement() != null) {
            Object encodedElement = propertyMeta.encode(listChangeAtIndex.getElement());
            return new ElementAtIndex(listChangeAtIndex.getIndex(), encodedElement);
        }
        return listChangeAtIndex;
    }

    public Set<Object> getEncodedSetChanges() {
        if (isNotEmpty(setChanges)) {
            return propertyMeta.encode(setChanges);
        }
        return setChanges;
    }

    public Map<Object, Object> getEncodedMapChanges() {
        if (MapUtils.isNotEmpty(mapChanges)) {
            Map<Object, Object> encodedMapChanges = new HashMap<>();
            for (Entry<Object, Object> entry : mapChanges.entrySet()) {
                Object encodedKey = propertyMeta.encodeKey(entry.getKey());
                Object encodedValue = propertyMeta.encode(entry.getValue());
                encodedMapChanges.put(encodedKey, encodedValue);
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

    public Pair<Assignments, Object[]> generateUpdateForAddedElements(Update.Conditions conditions, boolean preparedStatement) {
        Set<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(addAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedSetChanges();
            assignments = conditions.with(addAll(propertyName, encodedElements));
        }
        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForRemovedElements(Update.Conditions conditions, boolean preparedStatement) {
        Set<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(removeAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedSetChanges();
            assignments = conditions.with(removeAll(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForAppendedElements(Update.Conditions conditions, boolean preparedStatement) {
        List<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(appendAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            assignments = conditions.with(appendAll(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForPrependedElements(Update.Conditions conditions, boolean preparedStatement) {
        List<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(prependAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            assignments = conditions.with(prependAll(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForRemoveListElements(Update.Conditions conditions, boolean preparedStatement) {
        List<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(discardAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            assignments = conditions.with(discardAll(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForSetAtIndexElement(Update.Conditions conditions) {
        final ElementAtIndex elementAtIndex = getEncodedListChangeAtIndex();
        final int index = elementAtIndex.getIndex();
        final Object encoded = elementAtIndex.getElement();
        String propertyName = propertyMeta.getPropertyName();
        Assignments assignments = conditions.with(setIdx(propertyName, index, encoded));
        return Pair.create(assignments, new Object[] { index, encoded });
    }

    public Pair<Assignments, Object[]> generateUpdateForRemovedAtIndexElement(Update.Conditions conditions) {
        String propertyName = propertyMeta.getPropertyName();
        Assignments assignments = conditions.with(setIdx(propertyName, listChangeAtIndex.getIndex(), null));
        return Pair.create(assignments, new Object[] { listChangeAtIndex.getIndex(), null });
    }

    public Pair<Assignments, Object[]> generateUpdateForAddedEntries(Update.Conditions conditions, boolean preparedStatement) {
        Map<Object, Object> encodedEntries = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(putAll(propertyName, bindMarker(propertyName)));
        } else {
            encodedEntries = getEncodedMapChanges();
            assignments = conditions.with(putAll(propertyName, encodedEntries));
        }

        return Pair.create(assignments, new Object[] { encodedEntries });
    }

    public Pair<Assignments, Object[]> generateUpdateForRemovedKey(Update.Conditions conditions, boolean preparedStatement) {
        String propertyName = propertyMeta.getPropertyName();
        Assignments assignments;
        Object encodedKey = propertyMeta.encodeKey(mapChanges.keySet().iterator().next());
        if (preparedStatement) {
            assignments = conditions.with(put(propertyName, bindMarker("key"), bindMarker("nullValue")));
        } else {
            assignments = conditions.with(put(propertyName, encodedKey, null));
        }

        return Pair.create(assignments, new Object[] { encodedKey, null });
    }

    public Pair<Assignments, Object[]> generateUpdateForRemoveAll(Update.Conditions conditions, boolean preparedStatement) {
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(set(propertyName, bindMarker(propertyName)));
        } else {
            assignments = conditions.with(set(propertyName, null));
        }

        return Pair.create(assignments, new Object[] { null });
    }

    public Pair<Assignments, Object[]> generateUpdateForAssignValueToList(Update.Conditions conditions, boolean preparedStatement) {
        List<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(set(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedListChanges();
            assignments = conditions.with(set(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForAssignValueToSet(Update.Conditions conditions, boolean preparedStatement) {
        Set<Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(set(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedSetChanges();
            assignments = conditions.with(set(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
    }

    public Pair<Assignments, Object[]> generateUpdateForAssignValueToMap(Update.Conditions conditions, boolean preparedStatement) {
        Map<Object, Object> encodedElements = null;
        Assignments assignments;
        String propertyName = propertyMeta.getPropertyName();
        if (preparedStatement) {
            assignments = conditions.with(set(propertyName, bindMarker(propertyName)));
        } else {
            encodedElements = getEncodedMapChanges();
            assignments = conditions.with(set(propertyName, encodedElements));
        }

        return Pair.create(assignments, new Object[] { encodedElements });
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
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

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
