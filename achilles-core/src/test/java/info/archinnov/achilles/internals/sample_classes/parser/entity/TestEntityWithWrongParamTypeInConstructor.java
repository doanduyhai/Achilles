/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;

@Table
public class TestEntityWithWrongParamTypeInConstructor {

    @PartitionKey
    private Long id;

    @Column
    private String value;

    @EntityCreator
    public TestEntityWithWrongParamTypeInConstructor(long id, Double value) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public String getValue() {
        return value;
    }
}
