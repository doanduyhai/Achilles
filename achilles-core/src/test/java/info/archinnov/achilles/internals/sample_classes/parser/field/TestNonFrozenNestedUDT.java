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

import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;

@APUnitTest
@UDT(keyspace = "test", name = "non_frozen_nested_udt")
public class TestNonFrozenNestedUDT {

    @Column
    private String val;

    @Column
    private TestUDT udt;

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public TestUDT getUdt() {
        return udt;
    }

    public void setUdt(TestUDT udt) {
        this.udt = udt;
    }
}
