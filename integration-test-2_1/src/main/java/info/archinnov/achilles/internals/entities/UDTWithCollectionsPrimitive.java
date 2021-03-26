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
import java.util.Set;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.UDT;

@UDT(name = "udt_collections_primitive")
public class UDTWithCollectionsPrimitive {

    @Column
    private List<Integer> listInt;

    @Column
    private Set<Double> setDouble;

    public UDTWithCollectionsPrimitive() {
    }

    public UDTWithCollectionsPrimitive(List<Integer> listInt, Set<Double> setDouble) {
        this.listInt = listInt;
        this.setDouble = setDouble;
    }

    public List<Integer> getListInt() {
        return listInt;
    }

    public void setListInt(List<Integer> listInt) {
        this.listInt = listInt;
    }

    public Set<Double> getSetDouble() {
        return setDouble;
    }

    public void setSetDouble(Set<Double> setDouble) {
        this.setDouble = setDouble;
    }
}
