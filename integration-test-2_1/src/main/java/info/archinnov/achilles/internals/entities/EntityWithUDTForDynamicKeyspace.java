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

@Table(table = "table_with_with_dynamic_keyspace")
public class EntityWithUDTForDynamicKeyspace {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    @Frozen
    private UDTWithNoKeyspace clust;

    @Column
    @Frozen
    private UDTWithNoKeyspace udt;

    @Column
    private List<@Frozen UDTWithNoKeyspace> udtList;

    @Column
    private Set<@Frozen UDTWithNoKeyspace> udtSet;

    @Column
    private Map<@Frozen UDTWithNoKeyspace, String> udtMapKey;

    @Column
    private Map<Integer, @Frozen UDTWithNoKeyspace> udtMapValue;

    public EntityWithUDTForDynamicKeyspace() {
    }

    public EntityWithUDTForDynamicKeyspace(Long id, @Frozen UDTWithNoKeyspace clust, @Frozen UDTWithNoKeyspace udt) {
        this.id = id;
        this.clust = clust;
        this.udt = udt;
    }

    public EntityWithUDTForDynamicKeyspace(Long id, @Frozen UDTWithNoKeyspace clust, List<@Frozen UDTWithNoKeyspace> udtList) {
        this.id = id;
        this.clust = clust;
        this.udtList = udtList;
    }

    public EntityWithUDTForDynamicKeyspace(Long id, @Frozen UDTWithNoKeyspace clust, Set<@Frozen UDTWithNoKeyspace> udtSet) {
        this.id = id;
        this.clust = clust;
        this.udtSet = udtSet;
    }

    public EntityWithUDTForDynamicKeyspace(Long id, @Frozen UDTWithNoKeyspace clust, Map<@Frozen UDTWithNoKeyspace, String> udtMapKey) {
        this.id = id;
        this.clust = clust;
        this.udtMapKey = udtMapKey;
    }

    public EntityWithUDTForDynamicKeyspace(Map<Integer, @Frozen UDTWithNoKeyspace> udtMapValue, Long id, @Frozen UDTWithNoKeyspace clust) {
        this.id = id;
        this.clust = clust;
        this.udtMapValue = udtMapValue;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UDTWithNoKeyspace getClust() {
        return clust;
    }

    public void setClust(UDTWithNoKeyspace clust) {
        this.clust = clust;
    }

    public UDTWithNoKeyspace getUdt() {
        return udt;
    }

    public void setUdt(UDTWithNoKeyspace udt) {
        this.udt = udt;
    }

    public List<UDTWithNoKeyspace> getUdtList() {
        return udtList;
    }

    public void setUdtList(List<UDTWithNoKeyspace> udtList) {
        this.udtList = udtList;
    }

    public Set<UDTWithNoKeyspace> getUdtSet() {
        return udtSet;
    }

    public void setUdtSet(Set<UDTWithNoKeyspace> udtSet) {
        this.udtSet = udtSet;
    }

    public Map<UDTWithNoKeyspace, String> getUdtMapKey() {
        return udtMapKey;
    }

    public void setUdtMapKey(Map<UDTWithNoKeyspace, String> udtMapKey) {
        this.udtMapKey = udtMapKey;
    }

    public Map<Integer, UDTWithNoKeyspace> getUdtMapValue() {
        return udtMapValue;
    }

    public void setUdtMapValue(Map<Integer, UDTWithNoKeyspace> udtMapValue) {
        this.udtMapValue = udtMapValue;
    }
}