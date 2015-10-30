/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.injectable.InjectBeanFactory;
import info.archinnov.achilles.internals.injectable.InjectKeyspace;
import info.archinnov.achilles.internals.injectable.InjectUserTypeFactory;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class UDTProperty<ENTITY, A> extends AbstractProperty<ENTITY, A, UDTValue>
        implements InjectUserTypeFactory, InjectBeanFactory, InjectKeyspace {

    public static final TypeToken<UDTValue> UDT_VALUE_TYPE_TOKEN = new TypeToken<UDTValue>() {
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(UDTProperty.class);
    public final Class<A> valueClass;
    public final AbstractUDTClassProperty<A> udtClassProperty;

    public UDTProperty(FieldInfo<ENTITY, A> fieldInfo, Class<A> valueClass, AbstractUDTClassProperty<A> udtClassProperty) {
        super(TypeToken.of(valueClass), UDT_VALUE_TYPE_TOKEN, fieldInfo);
        this.valueClass = valueClass;
        this.udtClassProperty = udtClassProperty;
    }

    @Override
    public void encodeToSettable(UDTValue udt, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' UDT value %s to settable object %s",
                    fieldName, udt, settableData));
        }
        settableData.setUDTValue(fieldInfo.cqlColumn, udt);
    }

    @Override
    UDTValue encodeFromJavaInternal(A javaValue) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' %s to CQL UDT type", fieldName, javaValue));
        }
        return udtClassProperty.createUDTFromBean(javaValue);
    }

    @Override
    UDTValue encodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' object %s", fieldName, o));
        }

        Validator.validateTrue(valueClass.isAssignableFrom(o.getClass()), "The class of object {} to encode should be {}", o, valueClass.getCanonicalName());
        return encodeFromJava((A) o);
    }

    @Override
    A decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getUDTValue(fieldInfo.cqlColumn));
    }

    @Override
    A decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' raw object %s", fieldName, o));
        }

        Validator.validateTrue(UDTValue.class.isAssignableFrom(o.getClass()), "The class of object {} to decode should be {}", o, UDTValue.class.getCanonicalName());
        return udtClassProperty.createBeanFromUDT((UDTValue) o);
    }

    @Override
    public UserType buildType() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' UDT data type", fieldName));
        }

        return udtClassProperty.buildType();
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue) {
        final UDTValue valueTo = encodeField(entity);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' %s to UDT value %s", fieldName, valueTo, udtValue));
        }

        udtValue.setUDTValue(fieldInfo.cqlColumn, valueTo);
    }

    @Override
    public void inject(UserTypeFactory factory) {
        udtClassProperty.inject(factory);
    }

    @Override
    public void inject(BeanFactory factory) {
        udtClassProperty.inject(factory);
    }

    @Override
    public void injectKeyspace(String keyspace) {
        udtClassProperty.injectKeyspace(keyspace);
    }

    @Override
    public void inject(TupleTypeFactory factory) {
        udtClassProperty.inject(factory);
    }

    @Override
    public void inject(ObjectMapper mapper) {
        udtClassProperty.inject(mapper);
    }
}
