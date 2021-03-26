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

import info.archinnov.achilles.annotations.*;

@Table(table = "entity_with_no_ks_udt")
public class EntityWithNoKeyspaceUDT {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    @Frozen
    private NoKeyspaceUDT clust;

    @Column
    @Frozen
    private NoKeyspaceUDT udt;

    public EntityWithNoKeyspaceUDT() {
    }

    public EntityWithNoKeyspaceUDT(Long id, @Frozen NoKeyspaceUDT clust, @Frozen NoKeyspaceUDT udt) {
        this.id = id;
        this.clust = clust;
        this.udt = udt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public NoKeyspaceUDT getClust() {
        return clust;
    }

    public void setClust(NoKeyspaceUDT clust) {
        this.clust = clust;
    }

    public NoKeyspaceUDT getUdt() {
        return udt;
    }

    public void setUdt(NoKeyspaceUDT udt) {
        this.udt = udt;
    }
}
