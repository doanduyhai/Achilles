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
import java.util.Map;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;
import info.archinnov.achilles.type.tuples.Tuple2;

@Table(table = "entity_with_native_collections")
public class EntityWithNativeCollections {

    @PartitionKey
    private Long id;

    @Column
    private List<Long> longList;

    @Column
    private List<Double> doubleList;

    @Column
    private Map<Integer, Long> mapIntLong;

    @Column
    private Tuple2<List<Integer>, List<Double>> tuple2;

    public EntityWithNativeCollections(Long id, List<Long> longList, List<Double> doubleList, Map<Integer, Long> mapIntLong, Tuple2<List<Integer>, List<Double>> tuple2) {
        this.id = id;
        this.longList = longList;
        this.doubleList = doubleList;
        this.mapIntLong = mapIntLong;
        this.tuple2 = tuple2;
    }

    public EntityWithNativeCollections() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<Long> getLongList() {
        return longList;
    }

    public void setLongList(List<Long> longList) {
        this.longList = longList;
    }

    public List<Double> getDoubleList() {
        return doubleList;
    }

    public void setDoubleList(List<Double> doubleList) {
        this.doubleList = doubleList;
    }

    public Map<Integer, Long> getMapIntLong() {
        return mapIntLong;
    }

    public void setMapIntLong(Map<Integer, Long> mapIntLong) {
        this.mapIntLong = mapIntLong;
    }

    public Tuple2<List<Integer>, List<Double>> getTuple2() {
        return tuple2;
    }

    public void setTuple2(Tuple2<List<Integer>, List<Double>> tuple2) {
        this.tuple2 = tuple2;
    }
}
