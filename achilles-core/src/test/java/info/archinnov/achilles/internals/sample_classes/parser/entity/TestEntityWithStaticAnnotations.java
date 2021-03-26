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

import com.datastax.driver.core.ConsistencyLevel;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

@APUnitTest

@Table(keyspace = "my_ks")
@Strategy(naming = NamingStrategy.SNAKE_CASE, insert = InsertStrategy.NOT_NULL_FIELDS)
@Consistency(read = ConsistencyLevel.LOCAL_ONE, write = ConsistencyLevel.LOCAL_ONE, serial = ConsistencyLevel.LOCAL_SERIAL)
@TTL(123456)
public class TestEntityWithStaticAnnotations {

    @PartitionKey
    private Long partitionKey;

    @Column
    private String stringValue;

    @Column(value = "overriden")
    private String overridenName;

    public Long getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(Long partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getStringValue() {
        return stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public String getOverridenName() {
        return overridenName;
    }

    public void setOverridenName(String overridenName) {
        this.overridenName = overridenName;
    }
}
