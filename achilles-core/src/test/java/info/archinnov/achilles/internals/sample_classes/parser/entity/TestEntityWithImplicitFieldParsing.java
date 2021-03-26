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

package info.archinnov.achilles.internals.sample_classes.parser.entity;

import java.util.UUID;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@Table
public class TestEntityWithImplicitFieldParsing {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    private UUID uuid;

    private String value;

    @Transient
    private Long longValue;

    @Frozen
    private TestUDTImplicitFieldParsing udt;

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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getLongValue() {
        return longValue;
    }

    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    public TestUDTImplicitFieldParsing getUdt() {
        return udt;
    }

    public void setUdt(TestUDTImplicitFieldParsing udt) {
        this.udt = udt;
    }
}
