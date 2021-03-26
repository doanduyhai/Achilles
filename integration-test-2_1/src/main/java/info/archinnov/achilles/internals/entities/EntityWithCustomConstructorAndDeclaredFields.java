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

@Table(table = "custom_constructor_declared_fields")
public class EntityWithCustomConstructorAndDeclaredFields {

    @PartitionKey
    private Long id;

    @Column
    private String name;

    @Column
    private double value;

    @Column
    @Frozen
    private UDTWithCustomConstructorAndDeclaredFields udt;

    @EntityCreator({"id", "name"})
    public EntityWithCustomConstructorAndDeclaredFields(Long id, String myName) {
        this.id = id;
        this.name = myName;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public UDTWithCustomConstructorAndDeclaredFields getUdt() {
        return udt;
    }

    public void setUdt(UDTWithCustomConstructorAndDeclaredFields udt) {
        this.udt = udt;
    }
}
