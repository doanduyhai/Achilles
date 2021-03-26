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
import info.archinnov.achilles.internals.codecs.CodecOnClass;
import info.archinnov.achilles.internals.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.codecs.StringToLongCodec;
import info.archinnov.achilles.internals.types.ClassAnnotatedByCodec;
import info.archinnov.achilles.internals.types.IntWrapper;
import info.archinnov.achilles.type.tuples.Tuple1;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple3;

@Table(table = "entity_complex_types")
public class EntityWithComplexTypes {

    @PartitionKey
    private Long id;

    @Column
    private String value;

    @Enumerated(value = NAME)
    @Column
    private ConsistencyLevel consistencyLevel;

    @JSON
    @Column
    private Date time;

    @Column("primitive_bool")
    private boolean primitiveBoolean;

    @Column("object_bool")
    private Boolean objectBoolean;

    @Column("primitive_byte")
    private byte primitiveByte;

    @Column("object_byte")
    private Byte objectByte;

    @Column("primitive_byte_array")
    private byte[] primitiveByteArray;

    @Column("object_byte_array")
    private Byte[] objectByteArray;

    @Column
    @Codec(value = IntToStringCodec.class)
    private Integer integer;

    @Column("codec_on_class")
    private @Codec(CodecOnClass.class) ClassAnnotatedByCodec codecOnClass;

    @Column("simple_udt")
    private @Frozen TestUDT simpleUdt;

    @Column("list_udt")
    private List<@Frozen TestUDT> listUdt;

    @Column("map_udt")
    private Map<Integer, @Frozen TestUDT> mapUdt;

    @Column("ok_set")
    private Set<@Enumerated(value = ORDINAL) ConsistencyLevel> okSet;

    @Column("json_map")
    @JSON
    private Map<Integer, List<Integer>> jsonMap;

    @Column("map_with_nested_json")
    private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;

    @Column("list_nesting")
    private List<@Frozen Map<Integer, String>> listNesting;

    @Column("tuple_nesting")
    private Tuple2<Integer, List<String>> tupleNesting;

    @Column
    private Tuple1<@JSON ConsistencyLevel> tuple1;

    @Column
    private Tuple2<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer> tuple2;

    @Column("complex_nesting_map")
    @EmptyCollectionIfNull
    private Map<@JSON TestUDT,
            @EmptyCollectionIfNull @Frozen Map<Integer,
                    Tuple3<@Codec(value = IntToStringCodec.class) Integer,
                            Integer,
                            @Enumerated(value = ORDINAL) ConsistencyLevel>>> complexNestingMap;

    @Column("writetime")
    @Computed(function = "writetime", alias = "write_time", targetColumns = {"value"}, cqlClass = Long.class)
    private Long writeTime;

    @Column("writetime_with_codec")
    @Computed(function = "writetime", alias = "write_time_2", targetColumns = {"value"}, cqlClass = Long.class)
    @Codec(StringToLongCodec.class)
    private String writeTimeWithCodec;

    @Column
    @TimeUUID
    private UUID timeuuid;

    @Column
    private IntWrapper intWrapper;

    @Column
    private ProtocolVersion protocolVersion;

    @Column
    private Encoding encoding;

    @Column
    private double[] doubleArray;

    @Column
    private float[] floatArray;

    @Column
    private int[] intArray;

    @Column
    private long[] longArray;

    @Column
    private List<long[]> listOfLongArray;

    @Column
    private Instant jdkInstant;

    @Column
    private LocalDate jdkLocalDate;

    @Column
    private LocalTime jdkLocalTime;

    @Column
    private ZonedDateTime jdkZonedDateTime;

    @Enumerated(Encoding.ORDINAL)
    @Column
    private ProtocolVersion protocolVersionAsOrdinal;

    @Column
    private Optional<String> optionalString;

    @Column
    private Optional<ProtocolVersion> optionalProtocolVersion;

    @Column
    private Optional<@Enumerated(Encoding.ORDINAL) ProtocolVersion> optionalEncodingAsOrdinal;

    @Column
    private List<Optional<String>> listOfOptional;

    @ASCII
    @Column
    private String ascii;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

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

    public Integer getInteger() {
        return integer;
    }

    public void setInteger(Integer integer) {
        this.integer = integer;
    }

    public ClassAnnotatedByCodec getCodecOnClass() {
        return codecOnClass;
    }

    public void setCodecOnClass(ClassAnnotatedByCodec codecOnClass) {
        this.codecOnClass = codecOnClass;
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

    public Map<Integer, TestUDT> getMapUdt() {
        return mapUdt;
    }

    public void setMapUdt(Map<Integer, TestUDT> mapUdt) {
        this.mapUdt = mapUdt;
    }

    public Set<ConsistencyLevel> getOkSet() {
        return okSet;
    }

    public void setOkSet(Set<ConsistencyLevel> okSet) {
        this.okSet = okSet;
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

    public Tuple2<Integer, List<String>> getTupleNesting() {
        return tupleNesting;
    }

    public void setTupleNesting(Tuple2<Integer, List<String>> tupleNesting) {
        this.tupleNesting = tupleNesting;
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

    public Map<TestUDT, Map<Integer, Tuple3<Integer, Integer, ConsistencyLevel>>> getComplexNestingMap() {
        return complexNestingMap;
    }

    public void setComplexNestingMap(Map<TestUDT, Map<Integer, Tuple3<Integer, Integer, ConsistencyLevel>>> complexNestingMap) {
        this.complexNestingMap = complexNestingMap;
    }

    public Long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(Long writeTime) {
        this.writeTime = writeTime;
    }

    public String getWriteTimeWithCodec() {
        return writeTimeWithCodec;
    }

    public void setWriteTimeWithCodec(String writeTimeWithCodec) {
        this.writeTimeWithCodec = writeTimeWithCodec;
    }

    public UUID getTimeuuid() {
        return timeuuid;
    }

    public void setTimeuuid(UUID timeuuid) {
        this.timeuuid = timeuuid;
    }

    public IntWrapper getIntWrapper() {
        return intWrapper;
    }

    public void setIntWrapper(IntWrapper intWrapper) {
        this.intWrapper = intWrapper;
    }

    public ProtocolVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public Encoding getEncoding() {
        return encoding;
    }

    public void setEncoding(Encoding encoding) {
        this.encoding = encoding;
    }

    public double[] getDoubleArray() {
        return doubleArray;
    }

    public void setDoubleArray(double[] doubleArray) {
        this.doubleArray = doubleArray;
    }

    public float[] getFloatArray() {
        return floatArray;
    }

    public void setFloatArray(float[] floatArray) {
        this.floatArray = floatArray;
    }

    public int[] getIntArray() {
        return intArray;
    }

    public void setIntArray(int[] intArray) {
        this.intArray = intArray;
    }

    public long[] getLongArray() {
        return longArray;
    }

    public void setLongArray(long[] longArray) {
        this.longArray = longArray;
    }

    public List<long[]> getListOfLongArray() {
        return listOfLongArray;
    }

    public void setListOfLongArray(List<long[]> listOfLongArray) {
        this.listOfLongArray = listOfLongArray;
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

    public String getAscii() {
        return ascii;
    }

    public void setAscii(String ascii) {
        this.ascii = ascii;
    }
}
