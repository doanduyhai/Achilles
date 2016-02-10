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

package info.archinnov.achilles.internals.metamodel;

import static java.lang.String.format;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.codec.JSONCodec;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.internals.types.RuntimeCodecWrapper;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class SimpleProperty<ENTITY, VALUEFROM, VALUETO> extends AbstractProperty<ENTITY, VALUEFROM, VALUETO> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProperty.class);

    public final Codec<VALUEFROM, VALUETO> valueCodec;
    public final Function<GettableData, VALUETO> gettable;
    public final BiConsumer<SettableData, VALUETO> settable;
    public final DataType dataTypeInternal;

    public SimpleProperty(FieldInfo<ENTITY, VALUEFROM> fieldInfo, DataType dataType,
                          Function<GettableData, VALUETO> gettable,
                          BiConsumer<SettableData, VALUETO> settable,
                          TypeToken<VALUEFROM> valueFromTypeToken,
                          TypeToken<VALUETO> valueToTypeToken,
                          Codec<VALUEFROM, VALUETO> valueCodec) {
        super(valueFromTypeToken, valueToTypeToken, fieldInfo);
        this.dataTypeInternal = dataType;
        this.gettable = gettable;
        this.settable = settable;
        this.valueCodec = valueCodec;
    }

    @Override
    boolean isOptional() {
        return false;
    }

    @Override
    public void encodeToSettable(VALUETO valueTo, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' value %s to settable object %s",
                    fieldName, valueTo, settableData));
        }
        settable.accept(settableData, valueTo);
    }

    @Override
    public VALUETO encodeFromJavaInternal(VALUEFROM javaValue) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s'  %s to CQL type", fieldName, javaValue));
        }

        return valueCodec.encode(javaValue);
    }

    @Override
    public VALUETO encodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' object %s", fieldName, o));
        }

        Validator.validateTrue(valueFromTypeToken.getRawType().isAssignableFrom(o.getClass()), "The class of object {} to encode should be {}", o, valueFromTypeToken);
        return encodeFromJava((VALUEFROM) o);
    }

    @Override
    public VALUEFROM decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' from gettable object %s", fieldName, gettableData));
        }

        return valueCodec.decode(gettable.apply(gettableData));
    }


    @Override
    public VALUEFROM decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' raw object %s", fieldName, o));
        }

        Validator.validateTrue(valueToTypeToken.getRawType().isAssignableFrom(o.getClass()), "The class of object {} to decode should be {}", o, valueToTypeToken);
        return valueCodec.decode((VALUETO) o);
    }

    @Override
    public DataType buildType() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' data type", fieldName));
        }

        return dataTypeInternal;

    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue) {
        final VALUETO valueTo = encodeField(entity);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' %s to UDT value %s", fieldName, valueTo, udtValue));
        }

        settable.accept(udtValue, valueTo);
    }

    @Override
    public void inject(TupleTypeFactory factory) {
        // No op
    }

    @Override
    public void inject(UserTypeFactory factory) {
        // No op
    }

    @Override
    public void inject(ObjectMapper mapper) {
        if (valueCodec instanceof JSONCodec) {
            ((JSONCodec) valueCodec).setObjectMapper(mapper);
        }
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
}
