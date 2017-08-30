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

package info.archinnov.achilles.internals.sample_classes.parser.field;

import java.util.List;
import java.util.Map;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@UDT(keyspace = "test", name = "my_type")
public class TestUDT {

    @Column
    private String name;

    @Frozen
    @Column
    private List<String> list;

    @Frozen
    @Column
    private Map<@JSON Integer, String> map;

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
}
