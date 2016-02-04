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

import com.datastax.driver.core.DataType;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.codec.FallThroughCodec;
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
}
