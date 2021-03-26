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

@Table
public class EntityWithStaticColumn {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    private UUID uuid;

    @Static
    @Column("static_col")
    private String staticCol;

    @Static
    @Column("another_static_col")
    private String anotherStaticCol;

    @Column
    private String value;

    public EntityWithStaticColumn() {
    }

    public EntityWithStaticColumn(Long id, UUID uuid, String staticCol, String value) {
        this.id = id;
        this.uuid = uuid;
        this.staticCol = staticCol;
        this.value = value;
    }

    public EntityWithStaticColumn(Long id, UUID uuid, String staticCol, String anotherStaticCol, String value) {
        this.id = id;
        this.uuid = uuid;
        this.staticCol = staticCol;
        this.anotherStaticCol = anotherStaticCol;
        this.value = value;
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

    public String getStaticCol() {
        return staticCol;
    }

    public void setStaticCol(String staticCol) {
        this.staticCol = staticCol;
    }

    public String getAnotherStaticCol() {
        return anotherStaticCol;
    }

    public void setAnotherStaticCol(String anotherStaticCol) {
        this.anotherStaticCol = anotherStaticCol;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
