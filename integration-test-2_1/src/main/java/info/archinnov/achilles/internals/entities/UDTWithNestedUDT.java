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
import java.util.Objects;
import java.util.Optional;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.type.tuples.Tuple2;

@UDT(name = "having_nested_type")
public class UDTWithNestedUDT {

    @Column
    private String value;

    @Column
    private List<@Frozen UDTWithNoKeyspace> udtList;

    @Frozen
    @Column
    private UDTWithNoKeyspace nestedUDT;

    @Column
    private Tuple2<Integer, UDTWithNoKeyspace> tupleWithUDT;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<UDTWithNoKeyspace> getUdtList() {
        return udtList;
    }

    public void setUdtList(List<UDTWithNoKeyspace> udtList) {
        this.udtList = udtList;
    }

    public UDTWithNoKeyspace getNestedUDT() {
        return nestedUDT;
    }

    public void setNestedUDT(UDTWithNoKeyspace nestedUDT) {
        this.nestedUDT = nestedUDT;
    }

    public Tuple2<Integer, UDTWithNoKeyspace> getTupleWithUDT() {
        return tupleWithUDT;
    }

    public void setTupleWithUDT(Tuple2<Integer, UDTWithNoKeyspace> tupleWithUDT) {
        this.tupleWithUDT = tupleWithUDT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UDTWithNestedUDT that = (UDTWithNestedUDT) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(udtList, that.udtList) &&
                Objects.equals(nestedUDT, that.nestedUDT) &&
                Objects.equals(tupleWithUDT, that.tupleWithUDT);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, udtList, nestedUDT, tupleWithUDT);
    }
}
