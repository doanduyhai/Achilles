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

import com.datastax.driver.core.Duration;

import info.archinnov.achilles.annotations.*;

@Table(keyspace = "it_3_10", table = "entity_for_group_by")
@Immutable
public class EntityForGroupBy {

    @PartitionKey(1)
    public final Long id;

    @PartitionKey(2)
    @Column("uuID")
    public final UUID uuid;

    @ClusteringColumn(1)
    public final Integer clust1;

    @ClusteringColumn(2)
    @Column("clusteRing2")
    public final Integer clust2;

    @ClusteringColumn(3)
    public final Integer clust3;

    @Column
    public final Integer val;

    public EntityForGroupBy(Long id, UUID uuid, Integer clust1, Integer clust2, Integer clust3, Integer val) {
        this.id = id;
        this.uuid = uuid;
        this.clust1 = clust1;
        this.clust2 = clust2;
        this.clust3 = clust3;
        this.val = val;
    }
}
