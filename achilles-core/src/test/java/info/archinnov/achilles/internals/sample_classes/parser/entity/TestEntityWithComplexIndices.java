/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.sample_classes.parser.entity;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@Table
public class TestEntityWithComplexIndices {

    @PartitionKey
    private Long id;

    @Index
    @Column
    private String simpleIndex;


    @Index
    @Column
    private List<String> collectionIndex;

    @Index
    @Column
    @Frozen
    private Set<String> fullIndexOnCollection;

    @Column
    private Map<@Index String, String> indexOnMapKey;

    @Index
    @Column
    private Map<Integer, String> indexOnMapEntry;

    @Index(indexClassName = "java.lang.Long")
    private Long customIndex;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSimpleIndex() {
        return simpleIndex;
    }

    public void setSimpleIndex(String simpleIndex) {
        this.simpleIndex = simpleIndex;
    }

    public List<String> getCollectionIndex() {
        return collectionIndex;
    }

    public void setCollectionIndex(List<String> collectionIndex) {
        this.collectionIndex = collectionIndex;
    }

    public Set<String> getFullIndexOnCollection() {
        return fullIndexOnCollection;
    }

    public void setFullIndexOnCollection(Set<String> fullIndexOnCollection) {
        this.fullIndexOnCollection = fullIndexOnCollection;
    }

    public Map<String, String> getIndexOnMapKey() {
        return indexOnMapKey;
    }

    public void setIndexOnMapKey(Map<String, String> indexOnMapKey) {
        this.indexOnMapKey = indexOnMapKey;
    }

    public Map<Integer, String> getIndexOnMapEntry() {
        return indexOnMapEntry;
    }

    public void setIndexOnMapEntry(Map<Integer, String> indexOnMapEntry) {
        this.indexOnMapEntry = indexOnMapEntry;
    }

    public Long getCustomIndex() {
        return customIndex;
    }

    public void setCustomIndex(Long customIndex) {
        this.customIndex = customIndex;
    }
}
