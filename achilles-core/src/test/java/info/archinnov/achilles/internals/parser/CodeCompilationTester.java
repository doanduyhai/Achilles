/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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


import java.util.Optional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.codec.FallThroughCodec;
import info.archinnov.achilles.internals.metamodel.JdkOptionalProperty;
import info.archinnov.achilles.internals.metamodel.SimpleProperty;
import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs;

/**
 * This class purpose is to test that generated code does compile
 */
public class CodeCompilationTester {

    /** * Meta class for 'overridenName' property <br/> * The meta class exposes some useful methods: <ul> *    <li>encodeFromJava: encode a property from raw Java to CQL java compatible type </li> *    <li>encodeField: extract the current property value from the given info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs instance and encode to CQL java compatible type </li> *    <li>decodeFromGettable: decode from a {@link com.datastax.driver.core.GettableData} instance (Row, UDTValue, TupleValue) the current property</li> * </ul> */
    @java.lang.SuppressWarnings("serial")
    public static final SimpleProperty<TestEntityForCodecs, String, String> overridenName =
    new SimpleProperty<>(
        new FieldInfo<>((TestEntityForCodecs entity$) -> entity$.getOverridenName(),
            (TestEntityForCodecs entity$, java.lang.String value$) -> entity$.setOverridenName(value$),
                "overridenName", "\"overRiden\"",
                ColumnType.NORMAL, new ColumnInfo(false), IndexInfo.noIndex()),
                DataType.text(), gettableData$ -> gettableData$.get("\"overRiden\"", String.class),
                (settableData$, value$) -> settableData$.set("\"overRiden\"", value$, java.lang.String.class),
                new TypeToken<java.lang.String>(){}, new TypeToken<java.lang.String>(){},
                new FallThroughCodec<>(java.lang.String.class));


    /** * Meta class for 'jdkInstant' property <br/> * The meta class exposes some useful methods: <ul> *    <li>encodeFromJava: encode a property from raw Java to CQL java compatible type </li> *    <li>encodeField: extract the current property value from the given info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs instance and encode to CQL java compatible type </li> *    <li>decodeFromGettable: decode from a {@link com.datastax.driver.core.GettableData} instance (Row, UDTValue, TupleValue) the current property</li> * </ul> */
    @java.lang.SuppressWarnings("serial")
    public static final SimpleProperty<TestEntityForCodecs, java.time.Instant, java.time.Instant> jdkInstant =
    new SimpleProperty<>(
        new FieldInfo<>((TestEntityForCodecs entity$) -> entity$.getJdkInstant(),
            (TestEntityForCodecs entity$, java.time.Instant value$) -> entity$.setJdkInstant(value$),
            "jdkInstant", "jdk_instant", ColumnType.NORMAL, new ColumnInfo(false), IndexInfo.noIndex()),
        DataType.timestamp(), gettableData$ -> gettableData$.get("jdk_instant", java.time.Instant.class),
        (settableData$, value$) -> settableData$.set("jdk_instant", value$, java.time.Instant.class),
        new TypeToken<java.time.Instant>(){}, new TypeToken<java.time.Instant>(){},
        new FallThroughCodec<>(java.time.Instant.class));

    public static final JdkOptionalProperty<TestEntityForCodecs, ProtocolVersion, java.lang.String> optionalProtocolVersion =
    new JdkOptionalProperty<>(new FieldInfo<>((TestEntityForCodecs entity$) -> entity$.getOptionalProtocolVersion(),
    (TestEntityForCodecs entity$, java.util.Optional<ProtocolVersion> value$) -> entity$.setOptionalProtocolVersion(value$),
    "optionalProtocolVersion", "optional_protocol_version", ColumnType.NORMAL, new ColumnInfo(false), IndexInfo.noIndex()),
    new SimpleProperty<>(FieldInfo.<TestEntityForCodecs, ProtocolVersion> of("optionalProtocolVersion", "optional_protocol_version"),
    DataType.text(), gettable$ -> null, (udt$, value$) -> {}, new com.google.common.reflect.TypeToken<ProtocolVersion>(){}, new com.google.common.reflect.TypeToken<java.lang.String>(){}, new info.archinnov.achilles.internals.codec.EnumNameCodec<>(java.util.Arrays.asList(ProtocolVersion.values()), ProtocolVersion.class)));


}
