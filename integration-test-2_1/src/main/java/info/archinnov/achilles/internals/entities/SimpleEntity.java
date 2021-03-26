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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ConsistencyLevel;

import info.archinnov.achilles.annotations.*;

@Table(table = "simple")
public class SimpleEntity {

    @PartitionKey
    private Long id;

    @ClusteringColumn
    private Date date;

    @Column
    private String value;

    @Column
    private List<@Enumerated(value = Enumerated.Encoding.NAME) ConsistencyLevel> consistencyList;

    @Column
    private Set<Double> simpleSet;

    @Column
    private Map<Integer, String> simpleMap;

    public SimpleEntity() {
    }

    public SimpleEntity(Long id, Date date, String value) {
        this.id = id;
        this.date = date;
        this.value = value;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<ConsistencyLevel> getConsistencyList() {
        return consistencyList;
    }

    public void setConsistencyList(List<ConsistencyLevel> consistencyList) {
        this.consistencyList = consistencyList;
    }

    public Set<Double> getSimpleSet() {
        return simpleSet;
    }

    public void setSimpleSet(Set<Double> simpleSet) {
        this.simpleSet = simpleSet;
    }

    public Map<Integer, String> getSimpleMap() {
        return simpleMap;
    }

    public void setSimpleMap(Map<Integer, String> simpleMap) {
        this.simpleMap = simpleMap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleEntity{");
        sb.append("value='").append(value).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
