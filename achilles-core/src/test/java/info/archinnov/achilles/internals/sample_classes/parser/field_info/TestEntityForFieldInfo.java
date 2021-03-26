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

package info.archinnov.achilles.internals.sample_classes.parser.field_info;

import static info.archinnov.achilles.annotations.Enumerated.Encoding.NAME;
import static info.archinnov.achilles.annotations.Enumerated.Encoding.ORDINAL;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.sample_classes.APUnitTest;
import info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.type.tuples.Tuple3;

@APUnitTest
public class TestEntityForFieldInfo {

    @PartitionKey(0)
    private Long wrongPartitionOrder;

    @PartitionKey(1)
    @ClusteringColumn(1)
    private Long partitionAndClustering;

    @PartitionKey(1)
    @Static
    private Long partitionAndStatic;

    @PartitionKey(1)
    @Computed(function = "xx", alias = "xxx", targetColumns = {"xx"}, cqlClass = Object.class)
    private Long partitionAndComputed;

    @PartitionKey(1)
    private Long id;

    @PartitionKey(2)
    private UUID uuid;

    @ClusteringColumn(0)
    private String wrongClusteringOrder;

    @ClusteringColumn(0)
    @Static
    private String clusteringAndStatic;

    @ClusteringColumn(0)
    @Computed(function = "xx", alias = "xxx", targetColumns = {"xx"}, cqlClass = Object.class)
    private String clusteringAndComputed;

    @ClusteringColumn(1)
    private String clust1;

    @ClusteringColumn(value = 2, asc = false)
    private int clust2;

    @Static
    @Computed(function = "average", alias = "avg", targetColumns = {"staticCol"}, cqlClass = Integer.class)
    private int staticAndComputed;

    @Static
    private int staticCol;

    @Computed(function = "writetime", alias = "writetime", targetColumns = {"staticCol", "normal"}, cqlClass = Long.class)
    private Long computed;

    @Column
    private String normal;

    @Column
    @Frozen
    private TestUDT udt;

    @Index
    private List<String> indexedList;

    private List<@Index(name = "list_index") String> nestedIndexList;

    private List<String> notIndexedList;

    @Index(indexClassName = "java.lang.Long")
    private Map<Integer, String> indexedEntryMap;

    private Map<@Index Integer, String> indexedKeyMap;

    private Map<Integer, @Index String> indexedValueMap;

    private Map<@Index Integer, @Index String> duplicatedIndicesForMap;

    private Map<Integer, String> notIndexedMap;

    @Index
    private TestUDT indexedUdt;

    @Index
    @Frozen
    private List<String> indexedFrozenList;

    @Enumerated(value = NAME)
    private ConsistencyLevel consistencyLevel;

    private boolean primitiveBoolean;

    private Boolean objectBoolean;

    @EmptyCollectionIfNull
    private Map<@JSON TestUDT,
            @EmptyCollectionIfNull @Frozen Map<Integer,
                    Tuple3<@Codec(value = IntToStringCodec.class) Integer,
                            Integer,
                            @Enumerated(value = ORDINAL) ConsistencyLevel>>> map;

    @Codec(value = IntToStringCodec.class)
    private Integer integer;

    private TestUDT testUdt;

    private Set<@Enumerated(value = ORDINAL) ConsistencyLevel> set;

    @Counter
    private Long counter;

    @Static
    @Counter
    private Long staticCounter;

    @Column("UpperCase")
    private String upperCase;

    @Column
    private String columnWithNoSetter;

    @Column
    public final String immutableColumn;

    public TestEntityForFieldInfo() {
        this.immutableColumn = null;
    }

    public TestEntityForFieldInfo(String immutableColumn) {
        this.immutableColumn = immutableColumn;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
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

    public Map<TestUDT, Map<Integer, Tuple3<Integer, Integer, ConsistencyLevel>>> getMap() {
        return map;
    }

    public void setMap(Map<TestUDT, Map<Integer, Tuple3<Integer, Integer, ConsistencyLevel>>> map) {
        this.map = map;
    }

    public void setInteger(Integer integer) {
        this.integer = integer;
    }

    public TestUDT getTestUdt() {
        return testUdt;
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

    public String getUpperCase() {
        return upperCase;
    }

    public void setUpperCase(String upperCase) {
        this.upperCase = upperCase;
    }

    public String getColumnWithNoSetter() {
        return columnWithNoSetter;
    }
}
