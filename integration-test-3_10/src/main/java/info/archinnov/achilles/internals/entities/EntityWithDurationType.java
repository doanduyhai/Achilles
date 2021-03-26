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

import com.datastax.driver.core.Duration;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Immutable;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;

@Table(keyspace = "it_3_10", table = "entity_with_duration_type")
@Immutable
public class EntityWithDurationType {

    @PartitionKey
    public final Long id;

    @Column
    public final Duration duration;

    public EntityWithDurationType(Long id, Duration duration) {
        this.id = id;
        this.duration = duration;
    }
}
