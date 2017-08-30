/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

package info.archinnov.achilles.internals.metamodel;

import static info.archinnov.achilles.internals.cql.TupleExtractor.extractType;
import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.type.tuples.Tuple4;
import info.archinnov.achilles.validation.Validator;

public class Tuple4Property<ENTITY, A, B, C, D> extends AbstractTupleProperty<ENTITY, Tuple4<A, B, C, D>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tuple4.class);

    private final AbstractProperty<ENTITY, A, ?> aProperty;
    private final AbstractProperty<ENTITY, B, ?> bProperty;
    private final AbstractProperty<ENTITY, C, ?> cProperty;
    private final AbstractProperty<ENTITY, D, ?> dProperty;

    public Tuple4Property(FieldInfo<ENTITY, Tuple4<A, B, C, D>> fieldInfo, AbstractProperty<ENTITY, A, ?> aProperty, AbstractProperty<ENTITY, B, ?> bProperty, AbstractProperty<ENTITY, C, ?> cProperty, AbstractProperty<ENTITY, D, ?> dProperty) {
        super(new TypeToken<Tuple4<A, B, C, D>>() {
        }, fieldInfo);
        this.aProperty = aProperty;
        this.bProperty = bProperty;
        this.cProperty = cProperty;
        this.dProperty = dProperty;
    }

    @Override
    TupleValue encodeFromJavaInternal(Tuple4<A, B, C, D> tuple4, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' tuple4 %s to CQL type", fieldName, tuple4));
        }

        return getRuntimeTupleType(cassandraOptions).newValue(
                aProperty.encodeFromRaw(tuple4._1(), cassandraOptions),
                bProperty.encodeFromRaw(tuple4._2(), cassandraOptions),
                cProperty.encodeFromRaw(tuple4._3(), cassandraOptions),
                dProperty.encodeFromRaw(tuple4._4(), cassandraOptions));
    }

    @Override
    TupleValue encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' tuple4 object %s", fieldName, o));
        }

        Validator.validateTrue(Tuple4.class.isAssignableFrom(o.getClass()), "The class of object %s to encode should be Tuple4", o);
        return encodeFromJava((Tuple4<A, B, C, D>) o, cassandraOptions);
    }

    @Override
    Tuple4<A, B, C, D> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple4 from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getTupleValue(fieldInfo.quotedCqlColumn));
    }

    @Override
    Tuple4<A, B, C, D> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple4 raw object %s", fieldName, o));
        }

        Validator.validateTrue(TupleValue.class.isAssignableFrom(o.getClass()), "The class of object %s to decode should be %s", o, TupleValue.class.getCanonicalName());
        final List<DataType> types = tupleType.getComponentTypes();
        return new Tuple4<>(
                aProperty.decodeFromRaw(extractType((TupleValue) o, types.get(0), aProperty, 0)),
                bProperty.decodeFromRaw(extractType((TupleValue) o, types.get(1), bProperty, 1)),
                cProperty.decodeFromRaw(extractType((TupleValue) o, types.get(2), cProperty, 2)),
                dProperty.decodeFromRaw(extractType((TupleValue) o, types.get(3), dProperty, 3)));
    }

    @Override
    public TupleType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' tuple4 data type", fieldName));
        }

        return tupleTypeFactory.typeFor(
                aProperty.buildType(cassandraOptions),
                bProperty.buildType(cassandraOptions),
                cProperty.buildType(cassandraOptions),
                dProperty.buildType(cassandraOptions));
    }

    @Override
    protected List<AbstractProperty<ENTITY, ?, ?>> componentsProperty() {
        return Arrays.asList(aProperty, bProperty, cProperty, dProperty);
    }
}
