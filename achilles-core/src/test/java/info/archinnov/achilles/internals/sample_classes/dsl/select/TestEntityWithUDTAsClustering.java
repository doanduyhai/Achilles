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

package info.archinnov.achilles.internals.sample_classes.dsl.select;

import java.util.List;
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.annotations.*;

@Table(table = "udt_with_dynamic_keyspace")
public class TestEntityWithUDTAsClustering {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    @Frozen
    private TestUDTWithNoKeyspace clust;

    @Column
    @Frozen
    private TestUDTWithNoKeyspace udt;

    @Column
    private List<@Frozen TestUDTWithNoKeyspace> udtList;

    @Column
    private Set<@Frozen TestUDTWithNoKeyspace> udtSet;

    @Column
    private Map<@Frozen TestUDTWithNoKeyspace, String> udtMapKey;

    @Column
    private Map<Integer, @Frozen TestUDTWithNoKeyspace> udtMapValue;

    public TestEntityWithUDTAsClustering() {
    }

    public TestEntityWithUDTAsClustering(Long id, @Frozen TestUDTWithNoKeyspace clust, @Frozen TestUDTWithNoKeyspace udt) {
        this.id = id;
        this.clust = clust;
        this.udt = udt;
    }

    public TestEntityWithUDTAsClustering(Long id, @Frozen TestUDTWithNoKeyspace clust, List<@Frozen TestUDTWithNoKeyspace> udtList) {
        this.id = id;
        this.clust = clust;
        this.udtList = udtList;
    }

    public TestEntityWithUDTAsClustering(Long id, @Frozen TestUDTWithNoKeyspace clust, Set<@Frozen TestUDTWithNoKeyspace> udtSet) {
        this.id = id;
        this.clust = clust;
        this.udtSet = udtSet;
    }

    public TestEntityWithUDTAsClustering(Long id, @Frozen TestUDTWithNoKeyspace clust, Map<@Frozen TestUDTWithNoKeyspace, String> udtMapKey) {
        this.id = id;
        this.clust = clust;
        this.udtMapKey = udtMapKey;
    }

    public TestEntityWithUDTAsClustering(Map<Integer, @Frozen TestUDTWithNoKeyspace> udtMapValue, Long id, @Frozen TestUDTWithNoKeyspace clust) {
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

    public TestUDTWithNoKeyspace getClust() {
        return clust;
    }

    public void setClust(TestUDTWithNoKeyspace clust) {
        this.clust = clust;
    }

    public TestUDTWithNoKeyspace getUdt() {
        return udt;
    }

    public void setUdt(TestUDTWithNoKeyspace udt) {
        this.udt = udt;
    }

    public List<TestUDTWithNoKeyspace> getUdtList() {
        return udtList;
    }

    public void setUdtList(List<TestUDTWithNoKeyspace> udtList) {
        this.udtList = udtList;
    }

    public Set<TestUDTWithNoKeyspace> getUdtSet() {
        return udtSet;
    }

    public void setUdtSet(Set<TestUDTWithNoKeyspace> udtSet) {
        this.udtSet = udtSet;
    }

    public Map<TestUDTWithNoKeyspace, String> getUdtMapKey() {
        return udtMapKey;
    }

    public void setUdtMapKey(Map<TestUDTWithNoKeyspace, String> udtMapKey) {
        this.udtMapKey = udtMapKey;
    }

    public Map<Integer, TestUDTWithNoKeyspace> getUdtMapValue() {
        return udtMapValue;
    }

    public void setUdtMapValue(Map<Integer, TestUDTWithNoKeyspace> udtMapValue) {
        this.udtMapValue = udtMapValue;
    }
}