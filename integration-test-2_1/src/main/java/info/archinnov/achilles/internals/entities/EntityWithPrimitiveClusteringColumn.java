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

import java.util.Objects;

import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;

@Table(table = "entity_primitive_clustering")
public class EntityWithPrimitiveClusteringColumn {

    @PartitionKey
    private Long partition;

    @ClusteringColumn
    private boolean clustering;

    @Column
    private String value;

    public EntityWithPrimitiveClusteringColumn() {
    }

    public EntityWithPrimitiveClusteringColumn(Long partition, boolean clustering, String value) {
        this.partition = partition;
        this.clustering = clustering;
        this.value = value;
    }

    public Long getPartition() {
        return partition;
    }

    public void setPartition(Long partition) {
        this.partition = partition;
    }

    public boolean getClustering() {
        return clustering;
    }

    public void setClustering(boolean clustering) {
        this.clustering = clustering;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityWithPrimitiveClusteringColumn that = (EntityWithPrimitiveClusteringColumn) o;
        return clustering == that.clustering &&
                Objects.equals(partition, that.partition) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, clustering, value);
    }
}
