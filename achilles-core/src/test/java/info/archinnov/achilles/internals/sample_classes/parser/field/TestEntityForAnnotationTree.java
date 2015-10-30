/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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
import java.util.Map;
import java.util.Set;

import info.archinnov.achilles.annotations.EmptyCollectionIfNull;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.type.tuples.Tuple3;

@APUnitTest
public class TestEntityForAnnotationTree {

    @Enumerated(value = Enumerated.Encoding.NAME)
    private Long id;

    private @JSON Date time;

    private String value;

    private List<Integer> list;

    private TestUDT testUdt;

    private List<Map<Integer, String>> level1Nesting;

    private Set<@Enumerated(value = Enumerated.Encoding.NAME) Double> set;

    @EmptyCollectionIfNull
    @Frozen
    private Map<@JSON @Frozen Integer,
            Map<@Frozen Integer,
                    Tuple3<String,
                            @Frozen @EmptyCollectionIfNull Integer,
                            @Enumerated(value = Enumerated.Encoding.NAME) Date>>> map;


    @JSON
    private Map<@JSON Integer, List<Integer>> jsonMap;

    private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;
}





