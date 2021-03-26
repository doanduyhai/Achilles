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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.metamodel.columns.ComputedColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.types.RuntimeCodecWrapper;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class ComputedProperty<ENTITY, VALUEFROM, VALUETO> extends AbstractProperty<ENTITY, VALUEFROM, VALUETO> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputedProperty.class);

    public final ComputedColumnInfo computedColumnInfo;
    private final Function<GettableData, VALUETO> extractor;
    private final Codec<VALUEFROM, VALUETO> valueCodec;

    public ComputedProperty(FieldInfo<ENTITY, VALUEFROM> fieldInfo, Function<GettableData, VALUETO> extractor, Codec<VALUEFROM, VALUETO> valueCodec) {
        super(fieldInfo);
        this.valueFromTypeToken = new TypeToken<VALUEFROM>(getClass()) {
        };
        this.valueToTypeToken = new TypeToken<VALUETO>(getClass()) {
        };
        this.extractor = extractor;
        this.valueCodec = valueCodec;
        this.computedColumnInfo = (ComputedColumnInfo) fieldInfo.columnInfo;
    }

    @Override
    boolean isOptional() {
        return false;
    }

    @Override
    public void encodeToSettable(VALUETO valueto, SettableData<?> settableData) {
        throw new UnsupportedOperationException(format("Cannot set computed value to field '%s'", fieldInfo.fieldName));
    }

    @Override
    VALUETO encodeFromJavaInternal(VALUEFROM javaValue, Optional<CassandraOptions> cassandraOptions) {
        throw new UnsupportedOperationException(format("Cannot set computed value to field '%s'", fieldInfo.fieldName));
    }

    @Override
    VALUETO encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        throw new UnsupportedOperationException(format("Cannot set computed value to field '%s'", fieldInfo.fieldName));
    }

    @Override
    VALUEFROM decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode computed property %s from gettable data %s", this.toString(), gettableData));
        }
        return valueCodec.decode(extractor.apply(gettableData));
    }

    @Override
    VALUEFROM decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode computed property %s from raw data %s", this.toString(), o));
        }
        final Class<?> cqlClass = computedColumnInfo.cqlClass;
        Validator.validateTrue(cqlClass.isAssignableFrom(o.getClass()), "The class of object %s to decode should be %s", o, cqlClass.getCanonicalName());
        return valueCodec.decode((VALUETO) o);
    }

    @Override
    public DataType buildType(Optional<CassandraOptions> cassandraOptions) {
        throw new UnsupportedOperationException(format("No type for computed field '%s'", fieldInfo.fieldName));
    }

    @Override
    public String getColumnForSelect() {
        return computedColumnInfo.alias;
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue, Optional<CassandraOptions> cassandraOptions) {
        throw new UnsupportedOperationException(format("No UDT encoding for computed field '%s'", fieldInfo.fieldName));
    }

    @Override
    public boolean containsUDTProperty() {
        return false;
    }

    @Override
    public List<AbstractUDTClassProperty<?>> getUDTClassProperties() {
        return new ArrayList<>();
    }

    @Override
    public void inject(UserTypeFactory userTypeFactory, TupleTypeFactory tupleTypeFactory) {
        // No op
    }

    @Override
    public void inject(ObjectMapper mapper) {
        // No op
    }

    @Override
    public void inject(BeanFactory factory) {
        // No op
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        if (valueCodec instanceof RuntimeCodecWrapper) {
            ((RuntimeCodecWrapper)valueCodec).inject(runtimeCodecs);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ComputedProperty{");
        sb.append("computedColumnInfo=").append(computedColumnInfo);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void injectKeyspace(String keyspace) {
        // No op
    }
}
