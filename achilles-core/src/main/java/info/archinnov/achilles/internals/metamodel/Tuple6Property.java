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
import info.archinnov.achilles.type.tuples.Tuple6;
import info.archinnov.achilles.validation.Validator;

public class Tuple6Property<ENTITY, A, B, C, D, E, F> extends AbstractTupleProperty<ENTITY, Tuple6<A, B, C, D, E, F>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tuple6.class);

    private final AbstractProperty<ENTITY, A, ?> aProperty;
    private final AbstractProperty<ENTITY, B, ?> bProperty;
    private final AbstractProperty<ENTITY, C, ?> cProperty;
    private final AbstractProperty<ENTITY, D, ?> dProperty;
    private final AbstractProperty<ENTITY, E, ?> eProperty;
    private final AbstractProperty<ENTITY, F, ?> fProperty;

    public Tuple6Property(FieldInfo<ENTITY, Tuple6<A, B, C, D, E, F>> fieldInfo, AbstractProperty<ENTITY, A, ?> aProperty, AbstractProperty<ENTITY, B, ?> bProperty, AbstractProperty<ENTITY, C, ?> cProperty, AbstractProperty<ENTITY, D, ?> dProperty, AbstractProperty<ENTITY, E, ?> eProperty, AbstractProperty<ENTITY, F, ?> fProperty) {
        super(new TypeToken<Tuple6<A, B, C, D, E, F>>() {
        }, fieldInfo);
        this.aProperty = aProperty;
        this.bProperty = bProperty;
        this.cProperty = cProperty;
        this.dProperty = dProperty;
        this.eProperty = eProperty;
        this.fProperty = fProperty;
    }

    @Override
    TupleValue encodeFromJavaInternal(Tuple6<A, B, C, D, E, F> tuple6, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' tuple6 %s to CQL type", fieldName, tuple6));
        }

        return getRuntimeTupleType(cassandraOptions).newValue(
                aProperty.encodeFromRaw(tuple6._1(), cassandraOptions),
                bProperty.encodeFromRaw(tuple6._2(), cassandraOptions),
                cProperty.encodeFromRaw(tuple6._3(), cassandraOptions),
                dProperty.encodeFromRaw(tuple6._4(), cassandraOptions),
                eProperty.encodeFromRaw(tuple6._5(), cassandraOptions),
                fProperty.encodeFromRaw(tuple6._6(), cassandraOptions));
    }

    @Override
    TupleValue encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' tuple6 object %s", fieldName, o));
        }

        Validator.validateTrue(Tuple6.class.isAssignableFrom(o.getClass()), "The class of object %s to encode should be Tuple6", o);
        return encodeFromJava((Tuple6<A, B, C, D, E, F>) o, cassandraOptions);
    }

    @Override
    Tuple6<A, B, C, D, E, F> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple6 from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getTupleValue(fieldInfo.quotedCqlColumn));
    }

    @Override
    Tuple6<A, B, C, D, E, F> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple6 raw object %s", fieldName, o));
        }

        Validator.validateTrue(TupleValue.class.isAssignableFrom(o.getClass()), "The class of object %s to decode should be %s", o, TupleValue.class.getCanonicalName());
        final List<DataType> types = tupleType.getComponentTypes();
        return new Tuple6<>(
                aProperty.decodeFromRaw(extractType((TupleValue) o, types.get(0), aProperty, 0)),
                bProperty.decodeFromRaw(extractType((TupleValue) o, types.get(1), bProperty, 1)),
                cProperty.decodeFromRaw(extractType((TupleValue) o, types.get(2), cProperty, 2)),
                dProperty.decodeFromRaw(extractType((TupleValue) o, types.get(3), dProperty, 3)),
                eProperty.decodeFromRaw(extractType((TupleValue) o, types.get(4), eProperty, 4)),
                fProperty.decodeFromRaw(extractType((TupleValue) o, types.get(5), fProperty, 5)));
    }

    @Override
    public TupleType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' tuple6 data type", fieldName));
        }

        return tupleTypeFactory.typeFor(
                aProperty.buildType(cassandraOptions),
                bProperty.buildType(cassandraOptions),
                cProperty.buildType(cassandraOptions),
                dProperty.buildType(cassandraOptions),
                eProperty.buildType(cassandraOptions),
                fProperty.buildType(cassandraOptions));
    }

    @Override
    protected List<AbstractProperty<ENTITY, ?, ?>> componentsProperty() {
        return Arrays.asList(aProperty, bProperty, cProperty, dProperty, eProperty, fProperty);
    }
}
