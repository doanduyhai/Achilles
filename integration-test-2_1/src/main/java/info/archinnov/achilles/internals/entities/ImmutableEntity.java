/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import com.datastax.driver.core.ConsistencyLevel;
import info.archinnov.achilles.annotations.ClusteringColumn;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Table(table = "immutable_value")
public class ImmutableEntity {

    @PartitionKey
    private final long id;

    @ClusteringColumn
    private final Date date;

    @Column
    private final String value;

    @Column
    private final List<@Enumerated(value = Enumerated.Encoding.NAME) ConsistencyLevel> consistencyList;

    @Column
    private final Set<Double> simpleSet;

    @Column
    private final Map<Integer, String> simpleMap;

    @EntityCreator({"id", "date", "value", "consistencyList", "simpleSet", "simpleMap"})
    public ImmutableEntity(final long id, final Date date, final String value,
                           final List<ConsistencyLevel> consistencyList,
                           final Set<Double> simpleSet,
                           final Map<Integer, String> simpleMap) {
        this.id = id;
        this.date = date;
        this.value = value;
        this.consistencyList = consistencyList;
        this.simpleSet = simpleSet;
        this.simpleMap = simpleMap;
    }

    public long getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public String getValue() {
        return value;
    }

    public List<ConsistencyLevel> getConsistencyList() {
        return consistencyList;
    }

    public Set<Double> getSimpleSet() {
        return simpleSet;
    }

    public Map<Integer, String> getSimpleMap() {
        return simpleMap;
    }
}
