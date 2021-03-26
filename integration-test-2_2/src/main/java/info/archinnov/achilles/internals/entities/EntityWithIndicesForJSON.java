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

package info.archinnov.achilles.internals.entities;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.annotations.*;

@Table(table = "entity_with_indices_for_json")
public class EntityWithIndicesForJSON {

    @PartitionKey
    private Long id;

    @ClusteringColumn(1)
    private int clust1;

    @ClusteringColumn(2)
    private int clust2;

    @ClusteringColumn(3)
    private String clust3;

    @Index
    @Column
    private String simpleIndex;


    @Column
    private @Index List<String> collectionIndex;

    @Column
    private @Frozen @Index Set<String> fullIndexOnCollection;

    @Column
    private Map<@Index String, String> indexOnMapKey;

    @Column
    private Map<Integer, @Index String> indexOnMapValue;

    @Index
    @Column
    private Map<Integer, String> indexOnMapEntry;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getClust1() {
        return clust1;
    }

    public void setClust1(int clust1) {
        this.clust1 = clust1;
    }

    public int getClust2() {
        return clust2;
    }

    public void setClust2(int clust2) {
        this.clust2 = clust2;
    }

    public String getClust3() {
        return clust3;
    }

    public void setClust3(String clust3) {
        this.clust3 = clust3;
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

    public Map<Integer, String> getIndexOnMapValue() {
        return indexOnMapValue;
    }

    public void setIndexOnMapValue(Map<Integer, String> indexOnMapValue) {
        this.indexOnMapValue = indexOnMapValue;
    }

    public Map<Integer, String> getIndexOnMapEntry() {
        return indexOnMapEntry;
    }

    public void setIndexOnMapEntry(Map<Integer, String> indexOnMapEntry) {
        this.indexOnMapEntry = indexOnMapEntry;
    }
}
