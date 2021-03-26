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

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class JdkOptionalProperty<ENTITY, FROM, TO> extends AbstractProperty<ENTITY, Optional<FROM>, TO> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdkOptionalProperty.class);

    private final AbstractProperty<ENTITY, FROM, TO> aProperty;

    public JdkOptionalProperty(FieldInfo<ENTITY, Optional<FROM>> fieldInfo, AbstractProperty<ENTITY, FROM, TO> aProperty) {
        super(new TypeToken<Optional<FROM>>() {}.where(new TypeParameter<FROM>() {}, aProperty.valueFromTypeToken),
                aProperty.valueToTypeToken, fieldInfo);
        this.aProperty = aProperty;
    }

    @Override
    boolean isOptional() {
        return true;
    }

    @Override
    public void encodeToSettable(TO a, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode value %s to settable object %s", a, settableData));
        }
        if (a != null) {
            aProperty.encodeToSettable(a, settableData);
        }
    }

    @Override
    TO encodeFromJavaInternal(Optional<FROM> javaValue, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' optional object %s", fieldName, javaValue));
        }

        if (javaValue.isPresent()) {
            return aProperty.encodeFromJavaInternal(javaValue.get(), cassandraOptions);
        } else {
            return null;
        }
    }

    @Override
    TO encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' optional object %s", fieldName, o));
        }

        Validator.validateTrue(Optional.class.isAssignableFrom(o.getClass()), "The class of object %s to encode should be java.util.Optional", o);
        return encodeFromJava((Optional<FROM>) o, cassandraOptions);
    }

    @Override
    Optional<FROM> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' optional from gettable object %s", fieldName, gettableData));
        }

        final FROM decoded = aProperty.decodeFromGettableInternal(gettableData);
        if (decoded == null) {
            return Optional.empty();
        } else {
            return Optional.of(decoded);
        }
    }

    @Override
    Optional<FROM> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' tuple1 raw object %s", fieldName, o));
        }
        final FROM decoded = aProperty.decodeFromRawInternal(o);
        if (decoded == null) {
            return Optional.empty();
        } else {
            return Optional.of(decoded);
        }
    }

    @Override
    public DataType buildType(Optional<CassandraOptions> cassandraOptions) {
        return aProperty.buildType(cassandraOptions);
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue, Optional<CassandraOptions> cassandraOptions) {
        final TO encoded = aProperty.encodeField(entity, cassandraOptions);
        if (encoded != null) {
            aProperty.encodeFieldToUdt(entity, udtValue, cassandraOptions);
        }
    }

    @Override
    public boolean containsUDTProperty() {
        return aProperty.containsUDTProperty();
    }

    @Override
    public List<AbstractUDTClassProperty<?>> getUDTClassProperties() {
        return aProperty.getUDTClassProperties();
    }

    @Override
    public void inject(BeanFactory factory) {
        aProperty.inject(factory);
    }

    @Override
    public void inject(ObjectMapper mapper) {
        aProperty.inject(mapper);
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        aProperty.injectRuntimeCodecs(runtimeCodecs);
    }

    @Override
    public void inject(UserTypeFactory userTypeFactory, TupleTypeFactory tupleTypeFactory) {
        aProperty.inject(userTypeFactory, tupleTypeFactory);
    }

    @Override
    public void injectKeyspace(String keyspace) {
        aProperty.injectKeyspace(keyspace);
    }

    @Override
    public void inject(SchemaNameProvider schemaNameProvider) {
        super.inject(schemaNameProvider);
        this.aProperty.inject(schemaNameProvider);
    }
}
