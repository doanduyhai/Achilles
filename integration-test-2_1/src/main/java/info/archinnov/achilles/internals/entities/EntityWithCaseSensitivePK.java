/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

@Table(table = "entity_with_case_sensitive_pk")
public class EntityWithCaseSensitivePK {

    @Column("partitionKey")
    @PartitionKey
    private Long id;

    @Column("clusteringColumn")
    @ClusteringColumn
    private Long clust;

    @Column("listString")
    private List<String> list;

    @Column("setString")
    private Set<String> set;

    @Column("mapIntString")
    private Map<Integer, String> map;

    @Column("udtWithNoKeyspace")
    @Frozen
    private UDTWithNoKeyspace udt;

    public EntityWithCaseSensitivePK() {
    }

    public EntityWithCaseSensitivePK(Long id, Long clust) {
        this.id = id;
        this.clust = clust;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getClust() {
        return clust;
    }

    public void setClust(Long clust) {
        this.clust = clust;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public Set<String> getSet() {
        return set;
    }

    public void setSet(Set<String> set) {
        this.set = set;
    }

    public Map<Integer, String> getMap() {
        return map;
    }

    public void setMap(Map<Integer, String> map) {
        this.map = map;
    }

    public UDTWithNoKeyspace getUdt() {
        return udt;
    }

    public void setUdt(UDTWithNoKeyspace udt) {
        this.udt = udt;
    }
}
