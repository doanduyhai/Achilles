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

package info.archinnov.achilles.internals.config;

import static info.archinnov.achilles.annotations.Enumerated.Encoding.ORDINAL;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.datastax.driver.core.ConsistencyLevel;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.entities.TestUDT;
import info.archinnov.achilles.type.tuples.Tuple3;

@FunctionRegistry
public interface MyFunctionRegistry {

    Long convertToLong(String longValue);

    String convertListToJson(List<Optional<String>> strings);

    String convertConsistencyLevelList(List<@Enumerated ConsistencyLevel> consistencyLevels);

    String stringifyComplexNestingMap(Map<@JSON TestUDT,
            @EmptyCollectionIfNull @Frozen Map<Integer,
                    Tuple3<@Codec(value = IntToStringCodec.class) Integer,
                            Integer,
                            @Enumerated(value = ORDINAL) ConsistencyLevel>>> map);
}
