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

package info.archinnov.achilles.internals.sample_classes.parser.field;

import java.util.Date;
import java.util.List;

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.EntityCreator;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@UDT(keyspace = "test", name = "my_type_with_custom_constructor")
public class TestUDTWithCustomConstructor {

    @Column
    private String name;

    @Frozen
    @Column
    private List<String> list;

    @Column
    private Date date;

    @EntityCreator
    public TestUDTWithCustomConstructor(String name, List<String> list) {
        this.name = name;
        this.list = list;
    }

    public String getName() {
        return name;
    }

    public List<String> getList() {
        return list;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
