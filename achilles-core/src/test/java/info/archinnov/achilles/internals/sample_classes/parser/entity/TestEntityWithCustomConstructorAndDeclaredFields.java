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

import java.util.Date;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@Table
public class TestEntityWithCustomConstructorAndDeclaredFields {

    @PartitionKey
    private long id;

    @ClusteringColumn
    private Date date;

    @Column
    private Double value;

    @EntityCreator({"id", "date", "value"})
    public TestEntityWithCustomConstructorAndDeclaredFields(Long id, Date myDate, double myVal) {
        this.id = id;
        this.date = myDate;
        this.value = myVal;
    }

    public long getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public Double getValue() {
        return value;
    }
}
