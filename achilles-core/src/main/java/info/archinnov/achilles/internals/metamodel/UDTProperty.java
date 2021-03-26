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
import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import info.archinnov.achilles.internals.injectable.InjectUserAndTupleTypeFactory;
import info.archinnov.achilles.internals.metamodel.columns.FieldInfo;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.utils.CollectionsHelper;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class UDTProperty<ENTITY, UDT_META extends AbstractUDTClassProperty<A>, A> extends AbstractProperty<ENTITY, A, UDTValue>
        implements InjectUserAndTupleTypeFactory, InjectBeanFactory, InjectKeyspace {

    public static final TypeToken<UDTValue> UDT_VALUE_TYPE_TOKEN = new TypeToken<UDTValue>() {
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(UDTProperty.class);
    public final Class<A> valueClass;
    public final UDT_META udtClassProperty;

    public UDTProperty(FieldInfo<ENTITY, A> fieldInfo, Class<A> valueClass, UDT_META udtClassProperty) {
        super(TypeToken.of(valueClass), UDT_VALUE_TYPE_TOKEN, fieldInfo);
        this.valueClass = valueClass;
        this.udtClassProperty = udtClassProperty;
    }

    @Override
    boolean isOptional() {
        return false;
    }

    @Override
    public void encodeToSettable(UDTValue udt, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' UDT value %s to settable object %s",
                    fieldName, udt, settableData));
        }
        settableData.setUDTValue(fieldInfo.quotedCqlColumn, udt);
    }

    @Override
    UDTValue encodeFromJavaInternal(A javaValue, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' %s to CQL UDT type", fieldName, javaValue));
        }
        return udtClassProperty.createUDTFromBean(javaValue, fieldInfo.columnInfo.frozen, cassandraOptions);
    }

    @Override
    UDTValue encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' object %s", fieldName, o));
        }

        Validator.validateTrue(valueClass.isAssignableFrom(o.getClass()), "The class of object %s to encode should be %s", o, valueClass.getCanonicalName());
        return encodeFromJava((A) o, cassandraOptions);
    }

    @Override
    A decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getUDTValue(fieldInfo.quotedCqlColumn));
    }

    @Override
    A decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' raw object %s", fieldName, o));
        }

        Validator.validateTrue(UDTValue.class.isAssignableFrom(o.getClass()), "The class of object %s to decode should be %s", o, UDTValue.class.getCanonicalName());
        return udtClassProperty.createBeanFromUDT((UDTValue) o);
    }

    @Override
    public UserType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' UDT data type", fieldName));
        }

        return udtClassProperty.buildType(fieldInfo.columnInfo.frozen, cassandraOptions);
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue, Optional<CassandraOptions> cassandraOptions) {
        final UDTValue valueTo = encodeField(entity, cassandraOptions);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' %s to UDT value %s", fieldName, valueTo, udtValue));
        }

        udtValue.setUDTValue(fieldInfo.quotedCqlColumn, valueTo);
    }

    @Override
    public boolean containsUDTProperty() {
        return true;
    }

    @Override
    public List<AbstractUDTClassProperty<?>> getUDTClassProperties() {
        return CollectionsHelper.appendAll(
                udtClassProperty.componentsProperty
                        .stream()
                        .flatMap(x -> x.getUDTClassProperties().stream())
                        .collect(toList()),
                Arrays.asList(udtClassProperty));
    }

    @Override
    public void inject(UserTypeFactory userTypeFactory, TupleTypeFactory tupleTypeFactory) {
        udtClassProperty.inject(userTypeFactory, tupleTypeFactory);
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
    public void inject(ObjectMapper mapper) {
        udtClassProperty.inject(mapper);
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        udtClassProperty.injectRuntimeCodecs(runtimeCodecs);
    }

    @Override
    public void inject(SchemaNameProvider schemaNameProvider) {
        super.inject(schemaNameProvider);
        this.udtClassProperty.inject(schemaNameProvider);
    }
}
