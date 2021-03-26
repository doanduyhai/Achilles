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

import java.util.UUID;

import info.archinnov.achilles.annotations.*;

@Table(table = "entity_static_counter")
public class EntityWithStaticCounterColumn {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    private UUID uuid;

    @Static
    @Column("static_count")
    @Counter
    private Long staticCount;

    @Column
    @Counter
    private Long count;

    public EntityWithStaticCounterColumn() {
    }

    public EntityWithStaticCounterColumn(Long id, UUID uuid, Long staticCount, Long count) {
        this.id = id;
        this.uuid = uuid;
        this.staticCount = staticCount;
        this.count = count;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public Long getStaticCount() {
        return staticCount;
    }

    public void setStaticCount(Long staticCount) {
        this.staticCount = staticCount;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
