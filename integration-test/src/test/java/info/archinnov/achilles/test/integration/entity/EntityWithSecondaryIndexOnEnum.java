/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.test.integration.entity.EntityWithSecondaryIndexOnEnum.TABLE_NAME;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.type.ConsistencyLevel;

@Entity(table = TABLE_NAME)
public class EntityWithSecondaryIndexOnEnum {
    public static final String TABLE_NAME = "enum_secondary_index";

    @PartitionKey
    private Long id;

    @Index
    @Column(name = "\"consistencyLevel\"")
    private ConsistencyLevel consistencyLevel;

    public EntityWithSecondaryIndexOnEnum() {
    }

    public EntityWithSecondaryIndexOnEnum(Long id, ConsistencyLevel consistencyLevel) {
        this.id = id;
        this.consistencyLevel = consistencyLevel;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }
}
