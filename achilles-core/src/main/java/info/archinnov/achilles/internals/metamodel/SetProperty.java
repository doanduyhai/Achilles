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
import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class SetProperty<ENTITY, VALUEFROM, VALUETO> extends
        AbstractProperty<ENTITY, Set<VALUEFROM>, Set<VALUETO>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SetProperty.class);

    public final Class<?> valueToClass;
    public final AbstractProperty<ENTITY, VALUEFROM, VALUETO> valueProperty;
    public final boolean frozen;
    public final boolean emptyCollectionIfNull;

    public SetProperty(FieldInfo<ENTITY, Set<VALUEFROM>> fieldInfo, boolean frozen, boolean emptyCollectionIfNull, Class<?> valueToClass, AbstractProperty<ENTITY, VALUEFROM, VALUETO> valueProperty) {
        super(
                new TypeToken<Set<VALUEFROM>>() {
                }
                        .where(new TypeParameter<VALUEFROM>() {
                        }, valueProperty.valueFromTypeToken),
                new TypeToken<Set<VALUETO>>() {
                }
                        .where(new TypeParameter<VALUETO>() {
                        }, valueProperty.valueToTypeToken),
                fieldInfo);

        this.frozen = frozen;
        this.emptyCollectionIfNull = emptyCollectionIfNull;
        this.valueToClass = valueToClass;
        this.valueProperty = valueProperty;
    }

    @Override
    boolean isOptional() {
        return false;
    }

    public VALUETO encodeSingleElement(VALUEFROM javaValue) {
        return valueProperty.encodeFromRaw(javaValue);
    }

    @Override
    public void encodeToSettable(Set<VALUETO> valueTos, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' set value %s to settable object %s",
                    fieldName, valueTos, settableData));
        }
        settableData.setSet(fieldInfo.cqlColumn, valueTos);
    }

    @Override
    public Set<VALUETO> encodeFromJavaInternal(Set<VALUEFROM> set) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' set %s to CQL type", fieldName, set));
        }
        return new HashSet<>(set
                .stream()
                .map(value -> valueProperty.encodeFromRaw(value))
                .collect(toSet()));
    }

    @Override
    public Set<VALUETO> encodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' set object %s", fieldName, o));
        }

        Validator.validateTrue(Set.class.isAssignableFrom(o.getClass()), "The class of object {} to encode should be Set", o);
        return encodeFromJava((Set<VALUEFROM>) o);
    }

    @Override
    public Set<VALUEFROM> decodeFromGettable(GettableData gettableData) {
        if (gettableData.isNull(getColumnForSelect()) && !emptyCollectionIfNull) return null;
        return decodeFromGettableInternal(gettableData);
    }

    @Override
    public Set<VALUEFROM> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' set from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getSet(fieldInfo.cqlColumn, valueProperty.valueToTypeToken));
    }


    @Override
    public Set<VALUEFROM> decodeFromRaw(Object o) {
        return decodeFromRawInternal(o);
    }

    @Override
    public Set<VALUEFROM> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' set raw object %s", fieldName, o));
        }

        if (o == null) {
            if (emptyCollectionIfNull)
                return new HashSet<>();
            else
                return null;
        }

        Validator.validateTrue(Set.class.isAssignableFrom(o.getClass()), "The class of object {} to decode should be Set<{}>", o, valueToClass.getCanonicalName());

        return new HashSet<>(((Set<VALUETO>) o)
                .stream()
                .map(valueTo -> valueProperty.decodeFromRaw(valueTo))
                .collect(toSet()));
    }

    public VALUEFROM decodeSingleElement(VALUETO cassandraValue) {
        return valueProperty.decodeFromRaw(cassandraValue);
    }

    @Override
    public DataType buildType() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' set data type", fieldName));
        }

        final DataType valueType = valueProperty.buildType();
        if (frozen) {
            return DataType.frozenSet(valueType);
        } else {
            return DataType.set(valueType);
        }
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue) {
        final Set<VALUETO> valueTo = encodeField(entity);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' set %s to UDT value %s", fieldName, valueTo, udtValue));
        }
        udtValue.setSet(fieldInfo.cqlColumn, valueTo);
    }

    @Override
    public boolean containsUDTProperty() {
        return valueProperty.containsUDTProperty();
    }

    @Override
    public List<AbstractUDTClassProperty<?>> getUDTClassProperties() {
        return valueProperty.getUDTClassProperties();
    }

    @Override
    public void inject(TupleTypeFactory factory) {
        valueProperty.inject(factory);
    }

    @Override
    public void inject(UserTypeFactory factory) {
        valueProperty.inject(factory);
    }

    @Override
    public void inject(ObjectMapper mapper) {
        valueProperty.inject(mapper);
    }

    @Override
    public void inject(BeanFactory factory) {
        valueProperty.inject(factory);
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        valueProperty.injectRuntimeCodecs(runtimeCodecs);
    }
}
