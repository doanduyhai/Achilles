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
import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
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
import info.archinnov.achilles.internals.utils.CollectionsHelper;
import info.archinnov.achilles.internals.utils.NamingHelper;
import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;
import info.archinnov.achilles.type.factory.BeanFactory;
import info.archinnov.achilles.validation.Validator;

public class MapProperty<ENTITY, KEYFROM, KEYTO, VALUEFROM, VALUETO> extends
        AbstractProperty<ENTITY, Map<KEYFROM, VALUEFROM>, Map<KEYTO, VALUETO>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapProperty.class);

    public final AbstractProperty<ENTITY, KEYFROM, KEYTO> keyProperty;
    public final AbstractProperty<ENTITY, VALUEFROM, VALUETO> valueProperty;
    public final boolean frozen;
    public final boolean emptyCollectionIfNull;

    public MapProperty(FieldInfo<ENTITY, Map<KEYFROM, VALUEFROM>> fieldInfo,
                       boolean frozen, boolean emptyCollectionIfNull,
                       AbstractProperty<ENTITY, KEYFROM, KEYTO> keyProperty,
                       AbstractProperty<ENTITY, VALUEFROM, VALUETO> valueProperty) {
        super(new TypeToken<Map<KEYFROM, VALUEFROM>>() {
                }
                        .where(new TypeParameter<KEYFROM>() {
                        }, keyProperty.valueFromTypeToken)
                        .where(new TypeParameter<VALUEFROM>() {
                        }, valueProperty.valueFromTypeToken),
                new TypeToken<Map<KEYTO, VALUETO>>() {
                }
                        .where(new TypeParameter<KEYTO>() {
                        }, keyProperty.valueToTypeToken)
                        .where(new TypeParameter<VALUETO>() {
                        }, valueProperty.valueToTypeToken), fieldInfo);

        this.frozen = frozen;
        this.emptyCollectionIfNull = emptyCollectionIfNull;
        this.keyProperty = keyProperty;
        this.valueProperty = valueProperty;
    }

    /**
     * Encode the single key element of the map to a CQL-compatible value using Achilles codec system
     * @param javaValue
     * @return
     */
    public KEYTO encodeSingleKeyElement(KEYFROM javaValue) {
        return encodeSingleKeyElement(javaValue, Optional.empty());
    }

    /**
     * Encode the single key element of the map to a CQL-compatible value using Achilles codec system and a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * <br/>
     * static method to build such a CassandraOptions instance
     * @param javaValue
     * @param cassandraOptions
     * @return
     */
    public KEYTO encodeSingleKeyElement(KEYFROM javaValue, Optional<CassandraOptions> cassandraOptions) {
        return keyProperty.encodeFromRaw(javaValue, cassandraOptions);
    }

    /**
     * Encode the single value element of the map to a CQL-compatible value using Achilles codec system
     * @param javaValue
     * @return
     */
    public VALUETO encodeSingleValueElement(VALUEFROM javaValue) {
        return encodeSingleValueElement(javaValue, Optional.empty());
    }

    /**
     * Encode the single value element of the map to a CQL-compatible value using Achilles codec system and a CassandraOptions
     * containing a runtime SchemaNameProvider. Use the
     * <br/>
     * <br/>
     * <pre class="code"><code class="java">
     *     CassandraOptions.withSchemaNameProvider(SchemaNameProvider provider)
     * </code></pre>
     * <br/>
     * static method to build such a CassandraOptions instance
     * @param javaValue
     * @param cassandraOptions
     * @return
     */

    public VALUETO encodeSingleValueElement(VALUEFROM javaValue, Optional<CassandraOptions> cassandraOptions) {
        return valueProperty.encodeFromRaw(javaValue, cassandraOptions);
    }

    @Override
    boolean isOptional() {
        return false;
    }

    @Override
    public void encodeToSettable(Map<KEYTO, VALUETO> mapTo, SettableData<?> settableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' map value %s to settable object %s",
                    fieldName, mapTo, settableData));
        }
        settableData.setMap(fieldInfo.quotedCqlColumn, mapTo, keyProperty.valueToTypeToken, valueProperty.valueToTypeToken);
    }

    @Override
    public Map<KEYTO, VALUETO> encodeFromJavaInternal(Map<KEYFROM, VALUEFROM> map, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode from Java '%s' map %s to CQL type", fieldName, map));
        }
        return new HashMap<>(map
                .entrySet()
                .stream()
                .collect(toMap((Map.Entry entry) -> keyProperty.encodeFromRaw(entry.getKey(), cassandraOptions),
                        (Map.Entry entry) -> valueProperty.encodeFromRaw(entry.getValue(), cassandraOptions))));
    }

    @Override
    public Map<KEYTO, VALUETO> encodeFromRawInternal(Object o, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode raw '%s' map object %s", fieldName, o));
        }
        Validator.validateTrue(Map.class.isAssignableFrom(o.getClass()), "The class of object %s to encode should be Map", o);
        return encodeFromJava((Map<KEYFROM, VALUEFROM>) o, cassandraOptions);
    }

    @Override
    public Map<KEYFROM, VALUEFROM> decodeFromGettable(GettableData gettableData) {
        if (gettableData.isNull(NamingHelper.maybeQuote(getColumnForSelect())) && !emptyCollectionIfNull) return null;
        return decodeFromGettableInternal(gettableData);
    }

    @Override
    public Map<KEYFROM, VALUEFROM> decodeFromGettableInternal(GettableData gettableData) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' map from gettable object %s", fieldName, gettableData));
        }

        return decodeFromRaw(gettableData.getMap(fieldInfo.quotedCqlColumn, keyProperty.valueToTypeToken, valueProperty.valueToTypeToken));
    }

    @Override
    public Map<KEYFROM, VALUEFROM> decodeFromRaw(Object o) {
        return decodeFromRawInternal(o);
    }

    @Override
    public Map<KEYFROM, VALUEFROM> decodeFromRawInternal(Object o) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Decode '%s' map raw object %s", fieldName, o));
        }

        if (o == null) {
            if (emptyCollectionIfNull)
                return new HashMap<>();
            else
                return null;
        }

        Validator.validateTrue(Map.class.isAssignableFrom(o.getClass()), "The class of object %s to decode should be Map<%s,%s>", o,
                keyProperty.valueToTypeToken, valueProperty.valueToTypeToken);

        return new HashMap<>(((Map<KEYTO, VALUETO>) o)
                .entrySet()
                .stream()
                .collect(toMap((Map.Entry entry) -> keyProperty.decodeFromRaw(entry.getKey()), (Map.Entry entry) -> valueProperty.decodeFromRaw(entry.getValue()))));
    }

    public KEYFROM decodeSingleKeyElement(KEYTO cassandraValue) {
        return keyProperty.decodeFromRaw(cassandraValue);
    }

    public VALUEFROM decodeSingleValueElement(VALUETO cassandraValue) {
        return valueProperty.decodeFromRaw(cassandraValue);
    }

    @Override
    public DataType buildType(Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Build current '%s' map data type", fieldName));
        }
        final DataType keyType = keyProperty.buildType(cassandraOptions);
        final DataType valueType = valueProperty.buildType(cassandraOptions);
        if (frozen) {
            return DataType.frozenMap(keyType, valueType);
        } else {
            return DataType.map(keyType, valueType);
        }
    }

    @Override
    public void encodeFieldToUdt(ENTITY entity, UDTValue udtValue, Optional<CassandraOptions> cassandraOptions) {
        final Map<KEYTO, VALUETO> valueTo = encodeField(entity, cassandraOptions);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Encode '%s' map %s to UDT value %s", fieldName, valueTo, udtValue));
        }
        udtValue.set(fieldInfo.quotedCqlColumn, valueTo, valueToTypeToken);
    }

    @Override
    public boolean containsUDTProperty() {
        return keyProperty.containsUDTProperty() || valueProperty.containsUDTProperty();
    }

    @Override
    public List<AbstractUDTClassProperty<?>> getUDTClassProperties() {
        return CollectionsHelper.appendAll(keyProperty.getUDTClassProperties(), valueProperty.getUDTClassProperties());
    }

    @Override
    public void inject(UserTypeFactory userTypeFactory, TupleTypeFactory tupleTypeFactory) {
        keyProperty.inject(userTypeFactory, tupleTypeFactory);
        valueProperty.inject(userTypeFactory, tupleTypeFactory);
    }

    @Override
    public void inject(ObjectMapper mapper) {
        keyProperty.inject(mapper);
        valueProperty.inject(mapper);
    }

    @Override
    public void inject(BeanFactory factory) {
        keyProperty.inject(factory);
        valueProperty.inject(factory);
    }

    @Override
    public void injectRuntimeCodecs(Map<CodecSignature<?, ?>, Codec<?, ?>> runtimeCodecs) {
        keyProperty.injectRuntimeCodecs(runtimeCodecs);
        valueProperty.injectRuntimeCodecs(runtimeCodecs);
    }

    @Override
    public void injectKeyspace(String keyspace) {
        keyProperty.injectKeyspace(keyspace);
        valueProperty.injectKeyspace(keyspace);
    }

    @Override
    public void inject(SchemaNameProvider schemaNameProvider) {
        super.inject(schemaNameProvider);
        this.keyProperty.inject(schemaNameProvider);
        this.valueProperty.inject(schemaNameProvider);
    }
}
