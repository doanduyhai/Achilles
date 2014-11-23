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
package info.archinnov.achilles.test.integration.entity;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.JSON;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static info.archinnov.achilles.test.integration.entity.EntityWithJSONOnCollectionAndMap.TABLE_NAME;

@Entity(table = TABLE_NAME)
public class EntityWithJSONOnCollectionAndMap {
    public static final String TABLE_NAME = "entity_with_JSON_collection_map";

    @Id
    private Long id;

    @Column
    @JSON
    private List<Integer> myList;

    @Column
    @JSON
    private Set<Integer> mySet;

    @Column
    @JSON(key = true, value = false)
    private Map<Integer, Integer> keyMap;

    @Column
    @JSON
    private Map<Integer,Integer> valueMap;

    @Column
    @JSON(key = true)
    private Map<Integer,Integer> keyValueMap;

    public EntityWithJSONOnCollectionAndMap() {
    }

    public EntityWithJSONOnCollectionAndMap(Long id, List<Integer> myList, Set<Integer> mySet, Map<Integer, Integer> keyMap, Map<Integer, Integer> valueMap, Map<Integer, Integer> keyValueMap) {
        this.id = id;
        this.mySet = mySet;
        this.myList = myList;
        this.keyMap = keyMap;
        this.valueMap = valueMap;
        this.keyValueMap = keyValueMap;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<Integer> getMyList() {
        return myList;
    }

    public void setMyList(List<Integer> myList) {
        this.myList = myList;
    }

    public Set<Integer> getMySet() {
        return mySet;
    }

    public void setMySet(Set<Integer> mySet) {
        this.mySet = mySet;
    }

    public Map<Integer, Integer> getKeyMap() {
        return keyMap;
    }

    public void setKeyMap(Map<Integer, Integer> keyMap) {
        this.keyMap = keyMap;
    }

    public Map<Integer, Integer> getValueMap() {
        return valueMap;
    }

    public void setValueMap(Map<Integer, Integer> valueMap) {
        this.valueMap = valueMap;
    }

    public Map<Integer, Integer> getKeyValueMap() {
        return keyValueMap;
    }

    public void setKeyValueMap(Map<Integer, Integer> keyValueMap) {
        this.keyValueMap = keyValueMap;
    }
}
