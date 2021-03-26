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

package info.archinnov.achilles.internals.parser;


import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.codec.FallThroughCodec;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.metamodel.SimpleProperty;
import info.archinnov.achilles.internals.metamodel.columns.*;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithCustomConstructor;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.type.strategy.InsertStrategy;

/**
 * This class purpose is to test that generated code does compile
 */
public class CodeCompilationTester extends AbstractEntityProperty<TestEntityWithCustomConstructor> {

    /**
     * Meta class for 'id' property <br/>
     * The meta class exposes some useful methods: <ul>
     *    <li>encodeFromJava: encode a property from raw Java to CQL java compatible type </li>
     *    <li>encodeField: extract the current property value from the given TestEntityWithCustomConstructor instance and encode to CQL java compatible type </li>
     *    <li>decodeFromGettable: decode from a {@link com.datastax.driver.core.GettableData} instance (Row, UDTValue, TupleValue) the current property</li>
     * </ul>
     */
    @SuppressWarnings({"serial", "unchecked"})
    public static final SimpleProperty<TestEntityWithCustomConstructor, Long, Long> id = new SimpleProperty<TestEntityWithCustomConstructor, Long, Long>(new FieldInfo<>((TestEntityWithCustomConstructor entity$) -> entity$.getId(), (TestEntityWithCustomConstructor entity$, Long value$) -> {}, "id", "id", ColumnType.PARTITION, new PartitionKeyInfo(1, false), IndexInfo.noIndex()), DataType.bigint(), gettableData$ -> gettableData$.get("id", long.class), (settableData$, value$) -> settableData$.set("id", value$, long.class), new TypeToken<Long>(){}, new TypeToken<Long>(){}, new FallThroughCodec<>(Long.class));

    /**
     * Meta class for 'date' property <br/>
     * The meta class exposes some useful methods: <ul>
     *    <li>encodeFromJava: encode a property from raw Java to CQL java compatible type </li>
     *    <li>encodeField: extract the current property value from the given TestEntityWithCustomConstructor instance and encode to CQL java compatible type </li>
     *    <li>decodeFromGettable: decode from a {@link com.datastax.driver.core.GettableData} instance (Row, UDTValue, TupleValue) the current property</li>
     * </ul>
     */
    @SuppressWarnings({"serial", "unchecked"})
    public static final SimpleProperty<TestEntityWithCustomConstructor, Date, Date> date = new SimpleProperty<TestEntityWithCustomConstructor, Date, Date>(new FieldInfo<>((TestEntityWithCustomConstructor entity$) -> entity$.getDate(), (TestEntityWithCustomConstructor entity$, Date value$) -> {}, "date", "date", ColumnType.CLUSTERING, new ClusteringColumnInfo(1, false, ClusteringOrder.ASC), IndexInfo.noIndex()), DataType.timestamp(), gettableData$ -> gettableData$.get("date", java.util.Date.class), (settableData$, value$) -> settableData$.set("date", value$, java.util.Date.class), new TypeToken<Date>(){}, new TypeToken<Date>(){}, new FallThroughCodec<>(Date.class));

    /**
     * Meta class for 'value' property <br/>
     * The meta class exposes some useful methods: <ul>
     *    <li>encodeFromJava: encode a property from raw Java to CQL java compatible type </li>
     *    <li>encodeField: extract the current property value from the given TestEntityWithCustomConstructor instance and encode to CQL java compatible type </li>
     *    <li>decodeFromGettable: decode from a {@link com.datastax.driver.core.GettableData} instance (Row, UDTValue, TupleValue) the current property</li>
     * </ul>
     */
    @SuppressWarnings({"serial", "unchecked"})
    public static final SimpleProperty<TestEntityWithCustomConstructor, Double, Double> value = new SimpleProperty<TestEntityWithCustomConstructor, Double, Double>(new FieldInfo<>((TestEntityWithCustomConstructor entity$) -> entity$.getValue(), (TestEntityWithCustomConstructor entity$, Double value$) -> {}, "value", "value", ColumnType.NORMAL, new ColumnInfo(false), IndexInfo.noIndex()), DataType.cdouble(), gettableData$ -> gettableData$.get("value", java.lang.Double.class), (settableData$, value$) -> settableData$.set("value", value$, java.lang.Double.class), new TypeToken<Double>(){}, new TypeToken<Double>(){}, new FallThroughCodec<>(Double.class));

    /**
     * Static class to expose "TestEntityWithCustomConstructor_AchillesMeta" fields for <strong>type-safe</strong> function calls */
    public static final CodeCompilationTester.ColumnsForFunctions COLUMNS = new CodeCompilationTester.ColumnsForFunctions();

    @Override
    protected Class<TestEntityWithCustomConstructor> getEntityClass() {
        return TestEntityWithCustomConstructor.class;
    }

    @Override
    protected String getDerivedTableOrViewName() {
        return "testentitywithcustomconstructor";
    }

    @Override
    protected BiMap<String, String> fieldNameToCqlColumn() {
        BiMap<String,String> map = HashBiMap.create(3);
        map.put("id", "id");
        map.put("date", "date");
        map.put("value", "value");
        return map;
    }

    @Override
    protected Optional<ConsistencyLevel> getStaticReadConsistency() {
        return Optional.empty();
    }

    @Override
    protected Optional<InternalNamingStrategy> getStaticNamingStrategy() {
        return Optional.empty();
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getPartitionKeys() {
        return Arrays.asList(id);
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getClusteringColumns() {
        return Arrays.asList(date);
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getNormalColumns() {
        return Arrays.asList(value);
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getComputedColumns() {
        return Arrays.asList();
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getConstructorInjectedColumns() {
        return Arrays.asList(id,date,value);
    }

    @Override
    protected boolean isCounterTable() {
        return false;
    }

    @Override
    protected Optional<String> getStaticKeyspace() {
        return Optional.empty();
    }

    @Override
    protected Optional<String> getStaticTableOrViewName() {
        return Optional.empty();
    }

    @Override
    protected Optional<ConsistencyLevel> getStaticWriteConsistency() {
        return Optional.empty();
    }

    @Override
    protected Optional<ConsistencyLevel> getStaticSerialConsistency() {
        return Optional.empty();
    }

    @Override
    protected Optional<Integer> getStaticTTL() {
        return Optional.empty();
    }

    @Override
    protected Optional<InsertStrategy> getStaticInsertStrategy() {
        return Optional.empty();
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getStaticColumns() {
        return Arrays.asList();
    }

    @Override
    protected List<AbstractProperty<TestEntityWithCustomConstructor, ?, ?>> getCounterColumns() {
        return Arrays.asList();
    }

    @Override
    protected TestEntityWithCustomConstructor newInstanceFromCustomConstructor(final Row row, List<String> cqlColumns) {
        final long id_value = id.decodeFromGettable(row);
        final Date date_value = date.decodeFromGettable(row);
        final Double value_value = value.decodeFromGettable(row);
        return new TestEntityWithCustomConstructor(id_value,date_value,value_value);
    }

    /**
     * Utility class to expose all fields with their CQL type for function call */
    public static final class ColumnsForFunctions {


    }
}
