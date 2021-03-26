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

import static info.archinnov.achilles.annotations.Enumerated.Encoding.NAME;
import static info.archinnov.achilles.annotations.Enumerated.Encoding.ORDINAL;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.annotations.Enumerated.Encoding;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.sample_classes.codecs.StringToLongCodec;
import info.archinnov.achilles.internals.sample_classes.types.MyBean;
import info.archinnov.achilles.internals.sample_classes.types.SimpleLongWrapper;
import info.archinnov.achilles.type.tuples.*;

@APUnitTest
public class TestEntityForCodecs {

    @Enumerated(value = NAME)
    private ConsistencyLevel consistencyLevel;

    private @JSON Date time;

    private String value;

    private boolean primitiveBoolean;

    private Boolean objectBoolean;

    private byte primitiveByte;

    private Byte objectByte;

    private byte[] primitiveByteArray;

    private Byte[] objectByteArray;

    @Codec(value = IntToStringCodec.class)
    private Integer integer;

    @Codec(value = IntToStringCodec.class)
    private Integer okInteger;

    private List<Integer> list;

    @Frozen
    private TestUDT testUdt;

    @Frozen
    private TestUDT simpleUdt;

    private List<@Frozen TestUDT> listUdt;

    private Set<@Frozen TestUDT> setUdt;

    private Map<Integer, @Frozen TestUDT> mapUdt;

    private Set<@Enumerated(value = ORDINAL) ConsistencyLevel> set;

    private Set<@Enumerated(value = ORDINAL) ConsistencyLevel> okSet;

    @JSON
    private Map<@JSON Integer, List<Integer>> jsonMap;

    private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;

    private List<@Frozen Map<Integer, String>> listNesting;

    private Set<@Frozen Map<Integer, String>> setNesting;

    private Map<Integer, @Frozen List<String>> mapNesting;

    private Tuple2<Integer, List<String>> tupleNesting;

    private Tuple1<@JSON ConsistencyLevel> tuple1;

    @Column(value = "my_tuple_2")
    private Tuple2<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer> tuple2;

    private Tuple3<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer> tuple3;

    private Tuple4<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer> tuple4;

    private Tuple5<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer, Integer> tuple5;

    private Tuple6<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer, Integer, Integer> tuple6;

    private Tuple7<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer, Integer, Integer, Integer> tuple7;

    private Tuple8<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer, Integer, Integer, Integer, Integer> tuple8;

    private Tuple9<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple9;

    private Tuple10<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer,
            Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple10;

    private Map<Integer, Tuple2<Integer, String>> nestedTuple;

    @EmptyCollectionIfNull
    private Map<@JSON TestUDT,
            @Frozen @EmptyCollectionIfNull Map<Integer,
                    Tuple3<@Codec(value = IntToStringCodec.class) Integer,
                            Integer,
                            @Enumerated(value = ORDINAL) ConsistencyLevel>>> map;

    @Computed(function = "writetime", alias = "writetime", targetColumns = {"map"}, cqlClass = String.class)
    @Codec(IntToStringCodec.class)
    private Integer writeTimeAsInt;

    @Computed(function = "writetime", alias = "writetime", targetColumns = {"map"}, cqlClass = Long.class)
    private Long writeTime;

    @Computed(function = "writetime", alias = "writetime", targetColumns = {"map"}, cqlClass = Long.class)
    @Codec(IntToStringCodec.class)
    private Integer writeTimeAsLong;

    @Computed(function = "writetime", alias = "writetime", targetColumns = {"map"}, cqlClass = Long.class)
    private Integer writeTimeNotMatchingComputed;

    @Counter
    private Long counter;

    @Counter
    @Codec(StringToLongCodec.class)
    private String counterWithCodec;


    @Counter
    @Codec(IntToStringCodec.class)
    private Integer counterWithWrongCodec;

    @Column
    private SimpleLongWrapper longWrapper;

    @Column
    private MyBean myBean;

    @Column
    private ProtocolVersion protocolVersion;

    @RuntimeCodec(codecName = "protocol_version", cqlClass = String.class)
    @Column
    private ProtocolVersion runtimeCodec;

    @Column
    @Enumerated(Encoding.ORDINAL)
    private ProtocolVersion protocolVersionAsOrdinal;

    @Column
    private Encoding encoding;

    @Column
    private Map<@JSON Integer, double[]> mapOfDoubleArray;

    @Column
    private List<@Frozen Map<@Enumerated ProtocolVersion, List<int[]>>> nestedArrays;

    @Column("overRiden")
    private String overridenName;

    @Column
    private Instant jdkInstant;

    @Column
    private java.time.LocalDate jdkLocalDate;

    @Column
    private java.time.LocalTime jdkLocalTime;

    @Column
    private ZonedDateTime jdkZonedDateTime;

    @Column
    private Optional<String> optionalString;

    @Column
    private Optional<ProtocolVersion> optionalProtocolVersion;

    @Column
    private Optional<@Enumerated(Encoding.ORDINAL) ProtocolVersion> optionalEncodingAsOrdinal;

    @Column
    private List<Optional<String>> listOfOptional;

    @Column
    @Frozen
    private TestNestedUDT nestedUDT;

    @Column
    private TestNonFrozenNestedUDT nonFrozenNestedUDT;

    @Column
    private TestUDT nonFrozenUDT;

    @Column
    private TestUDTWithCounter udtWithCounter;

    @Column
    private TestUDTWithPartitionKey udtWithPartitionKey;

    @Column
    private TestUDTWithClusteringColumn udtWithClusteringColumn;

    @Column
    private TestUDTWithStaticColumn udtWithStaticColumn;

    @Column
    private TestUDTWithNonFrozenCollection udtWithNonFrozenCollection;

    @TimeUUID
    @Column
    private UUID timeuuid;

    @TimeUUID
    @Column
    private String wrongtimeuuid;

    @ASCII
    @Column
    private String ascii;

    @ASCII
    @Column
    private Integer wrongascii;

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public byte getPrimitiveByte() {
        return primitiveByte;
    }

    public void setPrimitiveByte(byte primitiveByte) {
        this.primitiveByte = primitiveByte;
    }

    public Byte getObjectByte() {
        return objectByte;
    }

    public void setObjectByte(Byte objectByte) {
        this.objectByte = objectByte;
    }

    public byte[] getPrimitiveByteArray() {
        return primitiveByteArray;
    }

    public void setPrimitiveByteArray(byte[] primitiveByteArray) {
        this.primitiveByteArray = primitiveByteArray;
    }

    public Byte[] getObjectByteArray() {
        return objectByteArray;
    }

    public void setObjectByteArray(Byte[] objectByteArray) {
        this.objectByteArray = objectByteArray;
    }

//    public Integer getInteger() {
//        return integer;
//    }

    public void setInteger(Integer integer) {
        this.integer = integer;
    }

    public List<Integer> getList() {
        return list;
    }

    public void setList(List<Integer> list) {
        this.list = list;
    }

    public TestUDT getTestUdt() {
        return testUdt;
    }

    public TestUDT getSimpleUdt() {
        return simpleUdt;
    }

    public void setSimpleUdt(TestUDT simpleUdt) {
        this.simpleUdt = simpleUdt;
    }

    public List<TestUDT> getListUdt() {
        return listUdt;
    }

    public void setListUdt(List<TestUDT> listUdt) {
        this.listUdt = listUdt;
    }

    public Set<TestUDT> getSetUdt() {
        return setUdt;
    }

    public void setSetUdt(Set<TestUDT> setUdt) {
        this.setUdt = setUdt;
    }

    public Map<Integer, TestUDT> getMapUdt() {
        return mapUdt;
    }

    public void setMapUdt(Map<Integer, TestUDT> mapUdt) {
        this.mapUdt = mapUdt;
    }

    public Set<ConsistencyLevel> getSet() {
        return set;
    }

    public void setSet(String whatever) {

    }

    public void setSet(Set<ConsistencyLevel> set, String whatever) {

    }

    public String setSet(Set<ConsistencyLevel> set) {
        return null;
    }


    public Map<Integer, List<Integer>> getJsonMap() {
        return jsonMap;
    }

    public void setJsonMap(Map<Integer, List<Integer>> jsonMap) {
        this.jsonMap = jsonMap;
    }

    public Map<Integer, List<Map<Integer, String>>> getMapWithNestedJson() {
        return mapWithNestedJson;
    }

    public void setMapWithNestedJson(Map<Integer, List<Map<Integer, String>>> mapWithNestedJson) {
        this.mapWithNestedJson = mapWithNestedJson;
    }

    public List<Map<Integer, String>> getListNesting() {
        return listNesting;
    }

    public void setListNesting(List<Map<Integer, String>> listNesting) {
        this.listNesting = listNesting;
    }

    public Set<Map<Integer, String>> getSetNesting() {
        return setNesting;
    }

    public void setSetNesting(Set<Map<Integer, String>> setNesting) {
        this.setNesting = setNesting;
    }

    public Map<Integer, List<String>> getMapNesting() {
        return mapNesting;
    }

    public void setMapNesting(Map<Integer, List<String>> mapNesting) {
        this.mapNesting = mapNesting;
    }

    public Tuple2<Integer, List<String>> getTupleNesting() {
        return tupleNesting;
    }

    public void setTupleNesting(Tuple2<Integer, List<String>> tupleNesting) {
        this.tupleNesting = tupleNesting;
    }

    public Map<Integer, Tuple2<Integer, String>> getNestedTuple() {
        return nestedTuple;
    }

    public void setNestedTuple(Map<Integer, Tuple2<Integer, String>> nestedTuple) {
        this.nestedTuple = nestedTuple;
    }

    public Map<TestUDT, Map<Integer, Tuple3<Integer, Integer, ConsistencyLevel>>> getMap() {
        return map;
    }

    public void setMap(Map<TestUDT, Map<Integer, Tuple3<Integer, Integer, ConsistencyLevel>>> map) {
        this.map = map;
    }

    public boolean isPrimitiveBoolean() {
        return primitiveBoolean;
    }

    public void setPrimitiveBoolean(boolean primitiveBoolean) {
        this.primitiveBoolean = primitiveBoolean;
    }

    public Boolean getObjectBoolean() {
        return objectBoolean;
    }

    public void setObjectBoolean(Boolean objectBoolean) {
        this.objectBoolean = objectBoolean;
    }

    public Integer getOkInteger() {
        return okInteger;
    }

    public void setOkInteger(Integer okInteger) {
        this.okInteger = okInteger;
    }

    public Set<ConsistencyLevel> getOkSet() {
        return okSet;
    }

    public void setOkSet(Set<ConsistencyLevel> okSet) {
        this.okSet = okSet;
    }

    public Tuple1<ConsistencyLevel> getTuple1() {
        return tuple1;
    }

    public void setTuple1(Tuple1<ConsistencyLevel> tuple1) {
        this.tuple1 = tuple1;
    }

    public Tuple2<ConsistencyLevel, Integer> getTuple2() {
        return tuple2;
    }

    public void setTuple2(Tuple2<ConsistencyLevel, Integer> tuple2) {
        this.tuple2 = tuple2;
    }

    public Tuple3<ConsistencyLevel, Integer, Integer> getTuple3() {
        return tuple3;
    }

    public void setTuple3(Tuple3<ConsistencyLevel, Integer, Integer> tuple3) {
        this.tuple3 = tuple3;
    }

    public Tuple4<ConsistencyLevel, Integer, Integer, Integer> getTuple4() {
        return tuple4;
    }

    public void setTuple4(Tuple4<ConsistencyLevel, Integer, Integer, Integer> tuple4) {
        this.tuple4 = tuple4;
    }

    public Tuple5<ConsistencyLevel, Integer, Integer, Integer, Integer> getTuple5() {
        return tuple5;
    }

    public void setTuple5(Tuple5<ConsistencyLevel, Integer, Integer, Integer, Integer> tuple5) {
        this.tuple5 = tuple5;
    }

    public Tuple6<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer> getTuple6() {
        return tuple6;
    }

    public void setTuple6(Tuple6<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer> tuple6) {
        this.tuple6 = tuple6;
    }

    public Tuple7<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer> getTuple7() {
        return tuple7;
    }

    public void setTuple7(Tuple7<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer> tuple7) {
        this.tuple7 = tuple7;
    }

    public Tuple8<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer, Integer> getTuple8() {
        return tuple8;
    }

    public void setTuple8(Tuple8<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple8) {
        this.tuple8 = tuple8;
    }

    public Tuple9<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> getTuple9() {
        return tuple9;
    }

    public void setTuple9(Tuple9<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple9) {
        this.tuple9 = tuple9;
    }

    public Tuple10<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> getTuple10() {
        return tuple10;
    }

    public void setTuple10(Tuple10<ConsistencyLevel, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple10) {
        this.tuple10 = tuple10;
    }

    public Integer getWriteTimeAsInt() {
        return writeTimeAsInt;
    }

    public void setWriteTimeAsInt(Integer writeTimeAsInt) {
        this.writeTimeAsInt = writeTimeAsInt;
    }

    public Long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(Long writeTime) {
        this.writeTime = writeTime;
    }

    public SimpleLongWrapper getLongWrapper() {
        return longWrapper;
    }

    public void setLongWrapper(SimpleLongWrapper longWrapper) {
        this.longWrapper = longWrapper;
    }

    public MyBean getMyBean() {
        return myBean;
    }

    public void setMyBean(MyBean myBean) {
        this.myBean = myBean;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public ProtocolVersion getRuntimeCodec() {
        return runtimeCodec;
    }

    public void setRuntimeCodec(ProtocolVersion runtimeCodec) {
        this.runtimeCodec = runtimeCodec;
    }

    public Encoding getEncoding() {
        return encoding;
    }

    public void setEncoding(Encoding encoding) {
        this.encoding = encoding;
    }

    public Map<Integer, double[]> getMapOfDoubleArray() {
        return mapOfDoubleArray;
    }

    public void setMapOfDoubleArray(Map<Integer, double[]> mapOfDoubleArray) {
        this.mapOfDoubleArray = mapOfDoubleArray;
    }

    public List<Map<ProtocolVersion, List<int[]>>> getNestedArrays() {
        return nestedArrays;
    }

    public void setNestedArrays(List<Map<ProtocolVersion, List<int[]>>> nestedArrays) {
        this.nestedArrays = nestedArrays;
    }

    public String getOverridenName() {
        return overridenName;
    }

    public void setOverridenName(String overridenName) {
        this.overridenName = overridenName;
    }

    public Instant getJdkInstant() {
        return jdkInstant;
    }

    public void setJdkInstant(Instant jdkInstant) {
        this.jdkInstant = jdkInstant;
    }

    public LocalDate getJdkLocalDate() {
        return jdkLocalDate;
    }

    public void setJdkLocalDate(LocalDate jdkLocalDate) {
        this.jdkLocalDate = jdkLocalDate;
    }

    public LocalTime getJdkLocalTime() {
        return jdkLocalTime;
    }

    public void setJdkLocalTime(LocalTime jdkLocalTime) {
        this.jdkLocalTime = jdkLocalTime;
    }

    public ZonedDateTime getJdkZonedDateTime() {
        return jdkZonedDateTime;
    }

    public void setJdkZonedDateTime(ZonedDateTime jdkZonedDateTime) {
        this.jdkZonedDateTime = jdkZonedDateTime;
    }

    public ProtocolVersion getProtocolVersionAsOrdinal() {
        return protocolVersionAsOrdinal;
    }

    public void setProtocolVersionAsOrdinal(ProtocolVersion protocolVersionAsOrdinal) {
        this.protocolVersionAsOrdinal = protocolVersionAsOrdinal;
    }

    public Optional<String> getOptionalString() {
        return optionalString;
    }

    public void setOptionalString(Optional<String> optionalString) {
        this.optionalString = optionalString;
    }

    public Optional<ProtocolVersion> getOptionalProtocolVersion() {
        return optionalProtocolVersion;
    }

    public void setOptionalProtocolVersion(Optional<ProtocolVersion> optionalProtocolVersion) {
        this.optionalProtocolVersion = optionalProtocolVersion;
    }

    public Optional<ProtocolVersion> getOptionalEncodingAsOrdinal() {
        return optionalEncodingAsOrdinal;
    }

    public void setOptionalEncodingAsOrdinal(Optional<ProtocolVersion> optionalEncodingAsOrdinal) {
        this.optionalEncodingAsOrdinal = optionalEncodingAsOrdinal;
    }

    public List<Optional<String>> getListOfOptional() {
        return listOfOptional;
    }

    public void setListOfOptional(List<Optional<String>> listOfOptional) {
        this.listOfOptional = listOfOptional;
    }

    public TestNestedUDT getNestedUDT() {
        return nestedUDT;
    }

    public void setNestedUDT(TestNestedUDT nestedUDT) {
        this.nestedUDT = nestedUDT;
    }

    public TestNonFrozenNestedUDT getNonFrozenNestedUDT() {
        return nonFrozenNestedUDT;
    }

    public void setNonFrozenNestedUDT(TestNonFrozenNestedUDT nonFrozenNestedUDT) {
        this.nonFrozenNestedUDT = nonFrozenNestedUDT;
    }

    public TestUDT getNonFrozenUDT() {
        return nonFrozenUDT;
    }

    public void setNonFrozenUDT(TestUDT nonFrozenUDT) {
        this.nonFrozenUDT = nonFrozenUDT;
    }

    public TestUDTWithCounter getUdtWithCounter() {
        return udtWithCounter;
    }

    public void setUdtWithCounter(TestUDTWithCounter udtWithCounter) {
        this.udtWithCounter = udtWithCounter;
    }

    public TestUDTWithPartitionKey getUdtWithPartitionKey() {
        return udtWithPartitionKey;
    }

    public void setUdtWithPartitionKey(TestUDTWithPartitionKey udtWithPartitionKey) {
        this.udtWithPartitionKey = udtWithPartitionKey;
    }

    public TestUDTWithClusteringColumn getUdtWithClusteringColumn() {
        return udtWithClusteringColumn;
    }

    public void setUdtWithClusteringColumn(TestUDTWithClusteringColumn udtWithClusteringColumn) {
        this.udtWithClusteringColumn = udtWithClusteringColumn;
    }

    public TestUDTWithStaticColumn getUdtWithStaticColumn() {
        return udtWithStaticColumn;
    }

    public void setUdtWithStaticColumn(TestUDTWithStaticColumn udtWithStaticColumn) {
        this.udtWithStaticColumn = udtWithStaticColumn;
    }

    public TestUDTWithNonFrozenCollection getUdtWithNonFrozenCollection() {
        return udtWithNonFrozenCollection;
    }

    public void setUdtWithNonFrozenCollection(TestUDTWithNonFrozenCollection udtWithNonFrozenCollection) {
        this.udtWithNonFrozenCollection = udtWithNonFrozenCollection;
    }

    public UUID getTimeuuid() {
        return timeuuid;
    }

    public void setTimeuuid(UUID timeuuid) {
        this.timeuuid = timeuuid;
    }

    public String getAscii() {
        return ascii;
    }

    public void setAscii(String ascii) {
        this.ascii = ascii;
    }

    public String getWrongtimeuuid() {
        return wrongtimeuuid;
    }

    public void setWrongtimeuuid(String wrongtimeuuid) {
        this.wrongtimeuuid = wrongtimeuuid;
    }

    public Integer getWrongascii() {
        return wrongascii;
    }

    public void setWrongascii(Integer wrongascii) {
        this.wrongascii = wrongascii;
    }
}





