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
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;

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
import info.archinnov.achilles.internals.injectable.InjectBeanFactory;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class ListProperty<ENTITY, VALUEFROM, VALUETO> extends
        AbstractProperty<ENTITY, List<VALUEFROM>, List<VALUETO>> implements InjectBeanFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListProperty.class);

    public final Class<?> valueToClass;
    public final AbstractProperty<ENTITY, VALUEFROM, VALUETO> valueProperty;
    public final boolean frozen;
    public final boolean emptyCollectionIfNull;

    public ListProperty(FieldInfo<ENTITY, List<VALUEFROM>> fieldInfo, boolean frozen, boolean emptyCollectionIfNull, Class<?> valueToClass, AbstractProperty<ENTITY, VALUEFROM, VALUETO> valueProperty) {
        super(
                new TypeToken<List<VALUEFROM>>() {
                }
                        .where(new TypeParameter<VALUEFROM>() {
                        }, valueProperty.valueFromTypeToken),
                new TypeToken<List<VALUETO>>() {
                }
                        .where(new TypeParameter<VALUETO>() {
                        }, valueProperty.valueToTypeToken),
                fieldInfo);

        this.frozen = frozen;
        this.emptyCollectionIfNull = emptyCollectionIfNull;
        this.valueToClass = valueToClass;
        this.valueProperty = valueProperty;
    }

    public VALUETO encodeSingleElement(VALUEFROM javaValue) {
        return valueProperty.encodeFromRaw(javaValue);
    }

    @Override
    public void encodeToSettable(List<VALUETO> valueTos, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode list '%s' value %s to settable object %s",
                    fieldName, valueTos, settableData));
        }

        settableData.setList(fieldInfo.cqlColumn, valueTos);
    }

    @Override
    public List<VALUETO> encodeFromJavaInternal(List<VALUEFROM> list) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' list %s to CQL type", fieldName, list));
        }

        return new ArrayList<>(list
                .stream()
                .map(value -> valueProperty.encodeFromJava(value))
                .collect(toList()));
    }

    @Override
    public List<VALUETO> encodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' list object %s", fieldName, o));
        }

        Validator.validateTrue(List.class.isAssignableFrom(o.getClass()), "The class of object {} to encode should be List", o);
        return encodeFromJava((List<VALUEFROM>) o);
    }

    @Override
    public List<VALUEFROM> decodeFromGettable(GettableData gettableData) {
        if (gettableData.isNull(getColumnForSelect()) && !emptyCollectionIfNull) return null;
        return decodeFromGettableInternal(gettableData);
    }

    @Override
    public List<VALUEFROM> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' list from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getList(fieldInfo.cqlColumn, valueProperty.valueToTypeToken));
    }

    @Override
    public List<VALUEFROM> decodeFromRaw(Object o) {
        return decodeFromRawInternal(o);
    }

    @Override
    public List<VALUEFROM> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' list raw object %s", fieldName, o));
        }

        if (o == null) {
            if (emptyCollectionIfNull)
                return new ArrayList<>();
            else
                return null;
        }

        Validator.validateTrue(List.class.isAssignableFrom(o.getClass()), "The class of object {} to decode should be List<{}>", o, List.class.getCanonicalName());

        return new ArrayList<>(((List<VALUETO>) o)
                .stream()
                .map(valueTo -> valueProperty.decodeFromRaw(valueTo))
                .collect(toList()));
    }

    public VALUEFROM decodeSingleElement(VALUETO cassandraValue) {
        return valueProperty.decodeFromRaw(cassandraValue);
    }

    @Override
    public DataType buildType() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' list data type", fieldName));
        }

        final DataType valueType = valueProperty.buildType();
        if (frozen) {
            return DataType.frozenList(valueType);
        } else {
            return DataType.list(valueType);
        }
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue) {

        final List<VALUETO> valueTo = encodeField(entity);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' list %s to UDT value %s", fieldName, valueTo, udtValue));
        }
        udtValue.setList(fieldInfo.cqlColumn, valueTo);
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
}
