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

import static com.datastax.driver.core.querybuilder.Update.Assignments;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

import org.apache.commons.collections.MapUtils;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.type.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            return propertyMeta.forTranscoding().encodeToCassandra(listChanges);
        }
        return listChanges;
    }

    public ElementAtIndex getEncodedListChangeAtIndex() {
        if (listChangeAtIndex != null && listChangeAtIndex.getElement() != null) {
            Object encodedElement = propertyMeta.forTranscoding().<List<Object>>encodeToCassandra(listChangeAtIndex.getElementAsList()).get(0);
            return new ElementAtIndex(listChangeAtIndex.getIndex(), encodedElement);
        }
        return listChangeAtIndex;
    }

    public Set<Object> getEncodedSetChanges() {
        if (isNotEmpty(setChanges)) {
            return propertyMeta.forTranscoding().encodeToCassandra(setChanges);
        }
        return setChanges;
    }

    public Map<Object, Object> getEncodedMapChanges() {
        if (MapUtils.isNotEmpty(mapChanges)) {
            return propertyMeta.forTranscoding().encodeToCassandra(mapChanges);
        }
        return mapChanges;
    }


    public CollectionAndMapChangeType getChangeType() {
        return changeType;
    }

    public PropertyMeta getPropertyMeta() {
        return propertyMeta;
    }

    public Assignments generateUpdateForAddedElements(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForAddedElements(conditions);
    }

    public Assignments generateUpdateForRemovedElements(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForRemovedElements(conditions);
    }

    public Assignments generateUpdateForAppendedElements(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForAppendedElements(conditions);
    }

    public Assignments generateUpdateForPrependedElements(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForPrependedElements(conditions);
    }

    public Assignments generateUpdateForRemoveListElements(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForRemoveListElements(conditions);
    }

    public Pair<Assignments, Object[]> generateUpdateForSetAtIndexElement(Update.Conditions conditions) {
        final ElementAtIndex elementAtIndex = getEncodedListChangeAtIndex();
        final int index = elementAtIndex.getIndex();
        final Object encoded = elementAtIndex.getElement();
        Assignments assignments = propertyMeta.forStatementGeneration().generateUpdateForSetAtIndexElement(conditions, index, encoded);
        return Pair.create(assignments, new Object[] { index, encoded });
    }

    public Pair<Assignments, Object[]> generateUpdateForRemovedAtIndexElement(Update.Conditions conditions) {
        Assignments assignments = propertyMeta.forStatementGeneration().generateUpdateForRemovedAtIndexElement(conditions, listChangeAtIndex.index);
        return Pair.create(assignments, new Object[] { listChangeAtIndex.index, null });
    }

    public Assignments generateUpdateForAddedEntries(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForAddedEntries(conditions);
    }

    public Assignments generateUpdateForRemovedKey(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForRemovedKey(conditions);
    }

    public Assignments generateUpdateForRemoveAll(Update.Conditions conditions) {
        return propertyMeta.forStatementGeneration().generateUpdateForRemoveAll(conditions);
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

    @Override
    public String toString() {
        return "DirtyCheckChangeSet{" +
                "changeType=" + changeType +
                ", propertyMeta=" + propertyMeta +
                ", listChanges=" + listChanges +
                ", listChangeAtIndex=" + listChangeAtIndex +
                ", setChanges=" + setChanges +
                ", mapChanges=" + mapChanges +
                '}';
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

        public List<Object> getElementAsList() {
            return Arrays.asList(element);
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
