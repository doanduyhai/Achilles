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

import java.util.List;
import javax.validation.Valid;

import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;

@Table(table = "bean_validation")
public class EntityWithBeanValidation {

    @PartitionKey
    private Long id;

    @Column
    @NotBlank(message = "should not be blank")
    private String value;

    @Column
    @NotEmpty(message = "should not be empty")
    private List<String> list;

    @Column
    @Valid
    @Frozen
    private TestUDT udt;

    public EntityWithBeanValidation() {
    }

    public EntityWithBeanValidation(Long id, String value, List<String> list, TestUDT udt) {
        this.id = id;
        this.value = value;
        this.list = list;
        this.udt = udt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public TestUDT getUdt() {
        return udt;
    }

    public void setUdt(TestUDT udt) {
        this.udt = udt;
    }
}
