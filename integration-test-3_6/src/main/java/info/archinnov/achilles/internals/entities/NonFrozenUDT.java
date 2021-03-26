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
import java.util.Set;

import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.UDT;

@UDT(keyspace = "it_3_6", name = "non_frozen_udt")
public class NonFrozenUDT {

    private String val;

    @Frozen
    private List<String> li;

    @Frozen
    private Set<String> se;

    @Frozen
    private Map<Integer, String> ma;

    @Frozen
    private AddressUDT address;

    public NonFrozenUDT() {
    }

    public NonFrozenUDT(String val, List<String> li, Set<String> se, Map<Integer, String> ma) {
        this.val = val;
        this.li = li;
        this.se = se;
        this.ma = ma;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public List<String> getLi() {
        return li;
    }

    public void setLi(List<String> li) {
        this.li = li;
    }

    public Set<String> getSe() {
        return se;
    }

    public void setSe(Set<String> se) {
        this.se = se;
    }

    public Map<Integer, String> getMa() {
        return ma;
    }

    public void setMa(Map<Integer, String> ma) {
        this.ma = ma;
    }

    public AddressUDT getAddress() {
        return address;
    }

    public void setAddress(AddressUDT address) {
        this.address = address;
    }
}
