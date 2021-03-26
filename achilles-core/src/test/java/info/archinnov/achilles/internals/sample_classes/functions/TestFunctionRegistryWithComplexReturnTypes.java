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

package info.archinnov.achilles.internals.sample_classes.functions;

import java.time.Instant;
import java.util.*;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.LocalDate;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.annotations.Enumerated.Encoding;
import info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.type.tuples.Tuple1;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple3;

@FunctionRegistry
public interface TestFunctionRegistryWithComplexReturnTypes {

    @Enumerated(Encoding.ORDINAL) ConsistencyLevel enumeratedParam();

    @JSON Date json();

    byte[] primitiveByteArray();

    Byte[] objectByteArray();

    @Codec(value = IntToStringCodec.class) Integer intToStringCodec();

    TestUDT udf();

    List<TestUDT> listUDT();

    Map<Integer, TestUDT> mapUDT();

    Set<@Enumerated ConsistencyLevel> setEnum();

    List<Map<Integer, String>> listOfMap();

    Tuple1<@JSON ConsistencyLevel> tuple1();

    Tuple2<Integer, List<@Codec(IntToStringCodec.class) Integer>> tuple2();

    Map<@JSON TestUDT, Map<Integer,
            Tuple3<@Codec(IntToStringCodec.class) Integer,
                    Integer,
                    @Enumerated(Encoding.ORDINAL) ConsistencyLevel>>> complicated();

    @TimeUUID UUID timeuuid();

    long[] longArray();
    int[] intArray();
    double[] doubleArray();
    float[] floatArray();

    LocalDate localDate();

    Instant jdkInstant();
    java.time.LocalDate jdkLocalDate();
    java.time.LocalTime jdkLocalTime();
    java.time.ZonedDateTime jdkZonedDateTime();
    Optional<String> jdkOptional();

}
