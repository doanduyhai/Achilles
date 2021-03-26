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

import java.util.Optional;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;

@Table(table = "table_with_nested_udt")
public class EntityWithNestedUDT {

    @PartitionKey
    private Long id;

    @Frozen
    @Column
    private UDTWithNoKeyspace udt;

    @Frozen
    @Column
    private UDTWithNestedUDT complexUDT;

    @Column
    private Optional<@Frozen UDTWithNoKeyspace> optionalUDT;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UDTWithNoKeyspace getUdt() {
        return udt;
    }

    public void setUdt(UDTWithNoKeyspace udt) {
        this.udt = udt;
    }

    public UDTWithNestedUDT getComplexUDT() {
        return complexUDT;
    }

    public void setComplexUDT(UDTWithNestedUDT complexUDT) {
        this.complexUDT = complexUDT;
    }

    public Optional<UDTWithNoKeyspace> getOptionalUDT() {
        return optionalUDT;
    }

    public void setOptionalUDT(Optional<UDTWithNoKeyspace> optionalUDT) {
        this.optionalUDT = optionalUDT;
    }
}
