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

package info.archinnov.achilles.internals.sample_classes.parser.strategy;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.type.tuples.*;

@APUnitTest
public class TestEntityWithNestedTypes {

    private TestUDT testUDT;

    private UDTValue udtValue;

    private TupleValue tupleValue;

    private List<List<String>> listList;

    private List<TestUDT> listUdt;

    private List<Tuple3<Integer, String, String>> listTuple;

    private List<TupleValue> listTupleValue;

    private List<UDTValue> listUdtValue;

    private Map<Integer, List<String>> mapList;

    private Map<Integer, TestUDT> mapUdt;

    private Map<Integer, Tuple2<Integer, String>> mapTuple;

    private Map<Integer, TupleValue> mapTupleValue;

    private Map<Integer, UDTValue> mapUDTValue;

    private Map<List<Integer>, String> mapListKey;

    private Map<TestUDT, String> mapUdtKey;

    private Map<Tuple4<Integer, Integer, String, String>, String> mapTupleKey;

    private Map<TupleValue, String> mapTupleValueKey;

    private Map<UDTValue, String> mapUDTValueKey;

    private Tuple5<Integer, List<String>, Integer, Integer, String> tupleList;

    private Tuple6<Integer, Integer, Integer, Integer, String, TestUDT> tupleUDT;

    private Tuple7<Integer, Integer, Integer, Integer, String, String, UDTValue> tupleUDTValue;

    private Tuple8<Integer, Integer, Integer, Integer, String, String, String, TupleValue> tupleTupleValue;

    private Tuple9<Integer, Integer, Integer, Integer, String, String, String, String, Map<Integer, String>> tupleMap;

    private Map<Integer, @Frozen @Index List<String>> indexedMapList;

    private Map<@Index Integer, @Frozen List<String>> indexedMapKey;

    @Index
    private String indexedString;

    private Map<Integer, @Frozen Map<Integer, @Index List<String>>> nestedIndex;
}
