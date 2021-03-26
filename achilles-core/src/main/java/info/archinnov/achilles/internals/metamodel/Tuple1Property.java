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
import info.archinnov.achilles.type.tuples.Tuple1;
import info.archinnov.achilles.validation.Validator;

public class Tuple1Property<ENTITY, A> extends AbstractTupleProperty<ENTITY, Tuple1<A>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Tuple1.class);

    private final AbstractProperty<ENTITY, A, ?> aProperty;

    public Tuple1Property(FieldInfo<ENTITY, Tuple1<A>> fieldInfo, AbstractProperty<ENTITY, A, ?> aProperty) {
        super(new TypeToken<Tuple1<A>>() {
        }, fieldInfo);
        this.aProperty = aProperty;
    }

    @Override
    public TupleValue encodeFromJavaInternal(Tuple1<A> tuple1, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' tuple1 %s to CQL type", fieldName, tuple1));
        }
        return getRuntimeTupleType(cassandraOptions)
                .newValue(aProperty.encodeFromRaw(tuple1._1(), cassandraOptions));
    }

    @Override
    public TupleValue encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' tuple1 object %s", fieldName, o));
        }

        Validator.validateTrue(Tuple1.class.isAssignableFrom(o.getClass()), "The class of object %s to encode should be Tuple1", o);
        return encodeFromJava((Tuple1<A>) o, cassandraOptions);
    }

    @Override
    public Tuple1<A> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple1 from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getTupleValue(fieldInfo.quotedCqlColumn));
    }

    @Override
    public Tuple1<A> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple1 raw object %s", fieldName, o));
        }
        Validator.validateTrue(TupleValue.class.isAssignableFrom(o.getClass()), "The class of object %s to decode should be %s", o, TupleValue.class.getCanonicalName());
        final List<DataType> types = tupleType.getComponentTypes();
        return new Tuple1<>(aProperty.decodeFromRaw(extractType((TupleValue) o, types.get(0), aProperty, 0)));
    }

    @Override
    public TupleType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' tuple1 data type", fieldName));
        }

        return tupleTypeFactory.typeFor(aProperty.buildType(cassandraOptions));
    }

    @Override
    protected List<AbstractProperty<ENTITY, ?, ?>> componentsProperty() {
        return Arrays.asList(aProperty);
    }
}
