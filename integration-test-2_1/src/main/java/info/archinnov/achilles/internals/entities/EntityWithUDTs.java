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
import java.util.Optional;
import java.util.Set;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.Table;
import info.archinnov.achilles.type.tuples.Tuple2;

@Table(table = "entity_with_udts")
public class EntityWithUDTs {

    @PartitionKey
    private Long id;

    @Column
    private List<@Frozen SimpleUDTWithNoKeyspace> listUDT;

    @Column
    private Set<@Frozen SimpleUDTWithNoKeyspace> setUDT;

    @Column
    private Map<@Frozen SimpleUDTWithNoKeyspace, @Frozen SimpleUDTWithNoKeyspace> mapUDT;

    @Column
    private Optional<@Frozen SimpleUDTWithNoKeyspace> optionalUDT;

    @Column
    private Tuple2<Integer, @Frozen SimpleUDTWithNoKeyspace> tupleUDT;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<SimpleUDTWithNoKeyspace> getListUDT() {
        return listUDT;
    }

    public void setListUDT(List<SimpleUDTWithNoKeyspace> listUDT) {
        this.listUDT = listUDT;
    }

    public Set<SimpleUDTWithNoKeyspace> getSetUDT() {
        return setUDT;
    }

    public void setSetUDT(Set<SimpleUDTWithNoKeyspace> setUDT) {
        this.setUDT = setUDT;
    }

    public Map<SimpleUDTWithNoKeyspace, SimpleUDTWithNoKeyspace> getMapUDT() {
        return mapUDT;
    }

    public void setMapUDT(Map<SimpleUDTWithNoKeyspace, SimpleUDTWithNoKeyspace> mapUDT) {
        this.mapUDT = mapUDT;
    }

    public Optional<SimpleUDTWithNoKeyspace> getOptionalUDT() {
        return optionalUDT;
    }

    public void setOptionalUDT(Optional<SimpleUDTWithNoKeyspace> optionalUDT) {
        this.optionalUDT = optionalUDT;
    }

    public Tuple2<Integer, SimpleUDTWithNoKeyspace> getTupleUDT() {
        return tupleUDT;
    }

    public void setTupleUDT(Tuple2<Integer, SimpleUDTWithNoKeyspace> tupleUDT) {
        this.tupleUDT = tupleUDT;
    }
}
