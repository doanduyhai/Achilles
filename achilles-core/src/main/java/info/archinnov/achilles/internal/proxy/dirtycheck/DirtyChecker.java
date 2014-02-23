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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ADD_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.APPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ASSIGN_VALUE_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ASSIGN_VALUE_TO_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.ASSIGN_VALUE_TO_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.PREPEND_TO_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_COLLECTION_OR_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_LIST_AT_INDEX;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_MAP;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.REMOVE_FROM_SET;
import static info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType.SET_TO_LIST_AT_INDEX;

public class DirtyChecker {

    protected PropertyMeta propertyMeta;
    protected List<DirtyCheckChangeSet> changeSets = new ArrayList<>();

    public static final Predicate<DirtyChecker> SIMPLE_FIELD = new Predicate<DirtyChecker>() {
        @Override
        public boolean apply(DirtyChecker dirtyChecker) {
            return dirtyChecker.isSimpleField();
        }
    };

    public static final Predicate<DirtyChecker> COLLECTION_AND_MAP_FIELD = new Predicate<DirtyChecker>() {
        @Override
        public boolean apply(DirtyChecker dirtyChecker) {
            return !dirtyChecker.isSimpleField();
        }
    };

    public static final Function<DirtyChecker,PropertyMeta> EXTRACT_META = new Function<DirtyChecker, PropertyMeta>() {
        @Override
        public PropertyMeta apply(DirtyChecker dirtyChecker) {
            return dirtyChecker.propertyMeta;
        }
    };

    public DirtyChecker(PropertyMeta propertyMeta) {
        this.propertyMeta = propertyMeta;
    }

    public boolean isSimpleField() {
        return false;
    }

    public PropertyMeta getPropertyMeta() {
        return propertyMeta;
    }

    public List<DirtyCheckChangeSet> getChangeSets() {
        return changeSets;
    }

    public void assignValue(List<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, ASSIGN_VALUE_TO_LIST);
        changeSet.setListChanges(elements);
        changeSets.add(changeSet);
    }

    public void assignValue(Set<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, ASSIGN_VALUE_TO_SET);
        changeSet.setSetChanges(elements);
        changeSets.add(changeSet);
    }

    public void assignValue(Map<Object, Object> entries) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, ASSIGN_VALUE_TO_MAP);
        changeSet.setMapChanges(entries);
        changeSets.add(changeSet);
    }

    public void removeAllElements() {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, REMOVE_COLLECTION_OR_MAP);
        changeSets.add(changeSet);
    }

    public void addElements(Set<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, ADD_TO_SET);
        changeSet.setSetChanges(elements);
        changeSets.add(changeSet);
        
    }

    public void removeElements(Set<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, REMOVE_FROM_SET);
        changeSet.setSetChanges(elements);
        changeSets.add(changeSet);
        
    }

    public void appendListElements(List<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, APPEND_TO_LIST);
        changeSet.setListChanges(elements);
        changeSets.add(changeSet);
        
    }

    public void prependListElements(List<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, PREPEND_TO_LIST);
        changeSet.setListChanges(elements);
        changeSets.add(changeSet);
        
    }

    public void removeElements(List<Object> elements) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, REMOVE_FROM_LIST);
        changeSet.setListChanges(elements);
        changeSets.add(changeSet);
        
    }

    public void setListElementAtIndex(int index,Object element) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, SET_TO_LIST_AT_INDEX);
        changeSet.setListChangeAtIndex(new DirtyCheckChangeSet.ElementAtIndex(index,element));
        changeSets.add(changeSet);
        
    }

    public void removeListElementAtIndex(int index) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, REMOVE_FROM_LIST_AT_INDEX);
        changeSet.setListChangeAtIndex(new DirtyCheckChangeSet.ElementAtIndex(index,null));
        changeSets.add(changeSet);
        
    }

    public void addElements(Map<Object, Object> entries) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, ADD_TO_MAP);
        changeSet.setMapChanges(entries);
        changeSets.add(changeSet);
        
    }

    public void removeMapEntry(Object key) {
        final DirtyCheckChangeSet changeSet = new DirtyCheckChangeSet(propertyMeta, REMOVE_FROM_MAP);
        HashMap<Object, Object> map = new HashMap<>();
        map.put(key,null);
        changeSet.setMapChanges(map);
        changeSets.add(changeSet);

    }
}
