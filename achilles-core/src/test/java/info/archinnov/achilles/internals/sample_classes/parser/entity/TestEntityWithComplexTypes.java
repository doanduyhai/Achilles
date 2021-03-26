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

package info.archinnov.achilles.internals.sample_classes.parser.entity;

import static info.archinnov.achilles.annotations.Enumerated.Encoding.NAME;
import static info.archinnov.achilles.annotations.Enumerated.Encoding.ORDINAL;

import java.util.*;

import com.datastax.driver.core.ConsistencyLevel;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.sample_classes.codecs.StringToLongCodec;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.type.tuples.Tuple1;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.type.tuples.Tuple3;

@APUnitTest
@Table
public class TestEntityWithComplexTypes {

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

    @Column
    private boolean primitiveBoolean;

    @Column
    private Boolean objectBoolean;

    @Column
    private byte primitiveByte;

    @Column
    private Byte objectByte;

    @Column
    private byte[] primitiveByteArray;

    @Column
    private Byte[] objectByteArray;

    @Column
    @Codec(value = IntToStringCodec.class)
    private Integer integer;

    @Column
    private @Frozen TestUDT simpleUdt;

    @Column
    private List<@Frozen TestUDT> listUdt;

    @Column
    private Map<Integer, @Frozen TestUDT> mapUdt;

    @Column
    private Set<@Enumerated(value = ORDINAL) ConsistencyLevel> okSet;

    @Column
    @JSON
    private Map<Integer, List<Integer>> jsonMap;

    @Column
    private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;

    @Column
    private List<@Frozen Map<Integer, String>> listNesting;

    @Column
    private Tuple2<Integer, List<String>> tupleNesting;

    @Column
    private Tuple1<@JSON ConsistencyLevel> tuple1;

    @Column
    private Tuple2<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer> tuple2;

    @Column
    @EmptyCollectionIfNull
    private Map<@JSON TestUDT,
            @EmptyCollectionIfNull @Frozen Map<Integer,
                    Tuple3<@Codec(value = IntToStringCodec.class) Integer,
                            Integer,
                            @Enumerated(value = ORDINAL) ConsistencyLevel>>> complexNestingMap;

    @Column
    @Computed(function = "writetime", alias = "write_time", targetColumns = {"value"}, cqlClass = Long.class)
    private Long writeTime;

    @Column
    @Computed(function = "writetime", alias = "write_time_2", targetColumns = {"value"}, cqlClass = Long.class)
    @Codec(StringToLongCodec.class)
    private String writeTimeWithCodec;

    @TimeUUID
    @Column
    private UUID timeuuid;

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
}
