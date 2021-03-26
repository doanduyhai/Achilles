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
public interface TestFunctionRegistryWithComplexTypes {

    String enumeratedParam(@Enumerated(Encoding.ORDINAL) ConsistencyLevel enumeratedParam);

    String json(@JSON Date json);

    String primitiveByteArray(byte[] primitiveByteArray);

    String objectByteArray(Byte[] objectByteArray);

    String intToStringCodec(@Codec(value = IntToStringCodec.class) Integer integer);

    String udf(@Frozen TestUDT udt);

    String listUDT(List<@Frozen TestUDT> listUdt);

    String mapUDT(Map<Integer, @Frozen TestUDT> mapUDT);

    String setEnum(Set<@Enumerated ConsistencyLevel> setEnum);

    String listOfMap(List<@Frozen Map<Integer, String>> listOfMap);

    String tuple1(Tuple1<@JSON ConsistencyLevel> tuple1);

    String tuple2(Tuple2<Integer, List<@Codec(IntToStringCodec.class) Integer>> tuple2);

    String complicated(Map<@JSON TestUDT, @Frozen Map<Integer,
                        Tuple3<@Codec(IntToStringCodec.class) Integer,
                                Integer,
                                @Enumerated(Encoding.ORDINAL) ConsistencyLevel>>> complicated);

    String timeuuid(@TimeUUID UUID timeuuid);

    String longArray(long[] longArray);
    String intArray(int[] intArray);
    String doubleArray(double[] doubleArray);
    String floatArray(float[] floatArray);

    String localDate(LocalDate localDate);

    String jdkInstant(Instant jdkInstant);
    String jdkLocalDate(java.time.LocalDate jdkLocalDate);
    String jdkLocalTime(java.time.LocalTime jdkLocalTime);
    String jdkZonedDateTime(java.time.ZonedDateTime jdkZonedDateTime);
    String jdkOptional(Optional<String> jdkOptional);

}
