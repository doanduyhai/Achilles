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
import info.archinnov.achilles.type.tuples.Tuple9;
import info.archinnov.achilles.validation.Validator;

public class Tuple9Property<ENTITY, A, B, C, D, E, F, G, H, I> extends AbstractTupleProperty<ENTITY, Tuple9<A, B, C, D, E, F, G, H, I>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tuple9.class);

    private final AbstractProperty<ENTITY, A, ?> aProperty;
    private final AbstractProperty<ENTITY, B, ?> bProperty;
    private final AbstractProperty<ENTITY, C, ?> cProperty;
    private final AbstractProperty<ENTITY, D, ?> dProperty;
    private final AbstractProperty<ENTITY, E, ?> eProperty;
    private final AbstractProperty<ENTITY, F, ?> fProperty;
    private final AbstractProperty<ENTITY, G, ?> gProperty;
    private final AbstractProperty<ENTITY, H, ?> hProperty;
    private final AbstractProperty<ENTITY, I, ?> iProperty;

    public Tuple9Property(FieldInfo<ENTITY, Tuple9<A, B, C, D, E, F, G, H, I>> fieldInfo,
                          AbstractProperty<ENTITY, A, ?> aProperty, AbstractProperty<ENTITY, B, ?> bProperty, AbstractProperty<ENTITY, C, ?> cProperty, AbstractProperty<ENTITY, D, ?> dProperty, AbstractProperty<ENTITY, E, ?> eProperty, AbstractProperty<ENTITY, F, ?> fProperty, AbstractProperty<ENTITY, G, ?> gProperty, AbstractProperty<ENTITY, H, ?> hProperty, AbstractProperty<ENTITY, I, ?> iProperty) {
        super(new TypeToken<Tuple9<A, B, C, D, E, F, G, H, I>>() {
        }, fieldInfo);
        this.aProperty = aProperty;
        this.bProperty = bProperty;
        this.cProperty = cProperty;
        this.dProperty = dProperty;
        this.eProperty = eProperty;
        this.fProperty = fProperty;
        this.gProperty = gProperty;
        this.hProperty = hProperty;
        this.iProperty = iProperty;
    }

    @Override
    TupleValue encodeFromJavaInternal(Tuple9<A, B, C, D, E, F, G, H, I> tuple9, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' tuple9 %s to CQL type", fieldName, tuple9));
        }

        return getRuntimeTupleType(cassandraOptions).newValue(
                aProperty.encodeFromRaw(tuple9._1(), cassandraOptions),
                bProperty.encodeFromRaw(tuple9._2(), cassandraOptions),
                cProperty.encodeFromRaw(tuple9._3(), cassandraOptions),
                dProperty.encodeFromRaw(tuple9._4(), cassandraOptions),
                eProperty.encodeFromRaw(tuple9._5(), cassandraOptions),
                fProperty.encodeFromRaw(tuple9._6(), cassandraOptions),
                gProperty.encodeFromRaw(tuple9._7(), cassandraOptions),
                hProperty.encodeFromRaw(tuple9._8(), cassandraOptions),
                iProperty.encodeFromRaw(tuple9._9(), cassandraOptions));
    }

    @Override
    TupleValue encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' tuple9 object %s", fieldName, o));
        }

        Validator.validateTrue(Tuple9.class.isAssignableFrom(o.getClass()), "The class of object %s to encode should be Tuple9", o);
        return encodeFromJava((Tuple9<A, B, C, D, E, F, G, H, I>) o, cassandraOptions);
    }

    @Override
    Tuple9<A, B, C, D, E, F, G, H, I> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple9 from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getTupleValue(fieldInfo.quotedCqlColumn));
    }

    @Override
    Tuple9<A, B, C, D, E, F, G, H, I> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple9 raw object %s", fieldName, o));
        }

        Validator.validateTrue(TupleValue.class.isAssignableFrom(o.getClass()), "The class of object %s to decode should be %s", o, TupleValue.class.getCanonicalName());
        final List<DataType> types = tupleType.getComponentTypes();
        return new Tuple9<>(
                aProperty.decodeFromRaw(extractType((TupleValue) o, types.get(0), aProperty, 0)),
                bProperty.decodeFromRaw(extractType((TupleValue) o, types.get(1), bProperty, 1)),
                cProperty.decodeFromRaw(extractType((TupleValue) o, types.get(2), cProperty, 2)),
                dProperty.decodeFromRaw(extractType((TupleValue) o, types.get(3), dProperty, 3)),
                eProperty.decodeFromRaw(extractType((TupleValue) o, types.get(4), eProperty, 4)),
                fProperty.decodeFromRaw(extractType((TupleValue) o, types.get(5), fProperty, 5)),
                gProperty.decodeFromRaw(extractType((TupleValue) o, types.get(6), gProperty, 6)),
                hProperty.decodeFromRaw(extractType((TupleValue) o, types.get(7), hProperty, 7)),
                iProperty.decodeFromRaw(extractType((TupleValue) o, types.get(8), iProperty, 7)));
    }

    @Override
    public TupleType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' tuple9 data type", fieldName));
        }

        return tupleTypeFactory.typeFor(
                aProperty.buildType(cassandraOptions),
                bProperty.buildType(cassandraOptions),
                cProperty.buildType(cassandraOptions),
                dProperty.buildType(cassandraOptions),
                eProperty.buildType(cassandraOptions),
                fProperty.buildType(cassandraOptions),
                gProperty.buildType(cassandraOptions),
                hProperty.buildType(cassandraOptions),
                iProperty.buildType(cassandraOptions));
    }

    @Override
    protected List<AbstractProperty<ENTITY, ?, ?>> componentsProperty() {
        return Arrays.asList(aProperty, bProperty, cProperty, dProperty, eProperty, fProperty, gProperty, hProperty, iProperty);
    }
}
