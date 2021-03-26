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

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.hibernate.validator.constraints.Email;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.constraints.ValidUDT;

@ValidUDT
@UDT(keyspace = DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME, name = "my_type")
public class TestUDT {

    @Email(message = "not a well-formed email address")
    @Column
    private String name;

    @Column
    private List<String> list;

    @Column
    private Map<@JSON Integer, String> map;

    public TestUDT() {
    }

    public TestUDT(String name, List<String> list, Map<@JSON Integer, String> map) {
        this.name = name;
        this.list = list;
        this.map = map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public Map<Integer, String> getMap() {
        return map;
    }

    public void setMap(Map<Integer, String> map) {
        this.map = map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestUDT testUDT = (TestUDT) o;
        return Objects.equals(name, testUDT.name) &&
                Objects.equals(list, testUDT.list) &&
                Objects.equals(map, testUDT.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, list, map);
    }
}
