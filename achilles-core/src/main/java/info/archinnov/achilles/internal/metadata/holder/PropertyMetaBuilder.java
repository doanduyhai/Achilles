/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.holder;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.google.common.base.Optional;
import info.archinnov.achilles.internal.metadata.transcoding.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.transcoding.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.transcoding.codec.SetCodec;
import info.archinnov.achilles.internal.metadata.transcoding.codec.SimpleCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

public class PropertyMetaBuilder {
    private static final Logger log = LoggerFactory.getLogger(PropertyMetaBuilder.class);

    private PropertyType type;
    private String propertyName;
    private String entityClassName;
    private Method[] accessors;
    private Field field;
    private ObjectMapper objectMapper;
    private CounterProperties counterProperties;

    private EmbeddedIdProperties embeddedIdProperties;
    private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
    private boolean timeUUID = false;
    private boolean emptyCollectionAndMapIfNull = false;
    private boolean staticColumn = false;
    private SimpleCodec simpleCodec;
    private ListCodec listCodec;
    private SetCodec setCodec;
    private MapCodec mapCodec;

    public static PropertyMetaBuilder factory() {
        return new PropertyMetaBuilder();
    }

    public PropertyMetaBuilder propertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    public PropertyMetaBuilder entityClassName(String entityClassName) {
        this.entityClassName = entityClassName;
        return this;
    }

    public PropertyMetaBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    public PropertyMeta build(Class<?> keyClass, Class<?> valueClass) {
        log.debug("Build propertyMeta for property {} of entity class {}", propertyName, entityClassName);

        PropertyMeta meta = new PropertyMeta();
        meta.setField(field);
        meta.setType(type);
        meta.setPropertyName(propertyName);
        meta.setEntityClassName(entityClassName);
        meta.setKeyClass(keyClass);
        meta.setValueClass(valueClass);
        meta.setGetter(accessors[0]);
        meta.setSetter(accessors[1]);
        meta.setEmbeddedIdProperties(embeddedIdProperties);
        meta.setCounterProperties(counterProperties);
        meta.setConsistencyLevels(consistencyLevels);
        meta.setTimeUUID(timeUUID);
        meta.setEmptyCollectionAndMapIfNull(emptyCollectionAndMapIfNull);
        meta.setStaticColumn(staticColumn);
        meta.setSimpleCodec(Optional.fromNullable(simpleCodec).orNull());
        meta.setListCodec(Optional.fromNullable(listCodec).orNull());
        meta.setSetCodec(Optional.fromNullable(setCodec).orNull());
        meta.setMapCodec(Optional.fromNullable(mapCodec).orNull());
        return meta;
    }

    public PropertyMetaBuilder type(PropertyType type) {
        this.type = type;
        return this;
    }

    public PropertyMetaBuilder accessors(Method[] accessors) {
        this.accessors = accessors;
        return this;
    }

    public PropertyMetaBuilder field(Field field) {
        this.field = field;
        return this;
    }

    public PropertyMetaBuilder embeddedIdProperties(EmbeddedIdProperties embeddedIdProperties) {
        this.embeddedIdProperties = embeddedIdProperties;
        return this;
    }

    public PropertyMetaBuilder counterProperties(CounterProperties counterProperties) {
        this.counterProperties = counterProperties;
        return this;
    }

    public PropertyMetaBuilder consistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
        this.consistencyLevels = consistencyLevels;
        return this;
    }

    public PropertyMetaBuilder timeuuid(boolean timeUUID) {
        this.timeUUID = timeUUID;
        return this;
    }

    public PropertyMetaBuilder staticColumn(boolean staticColumn) {
        this.staticColumn = staticColumn;
        return this;
    }

    public PropertyMetaBuilder emptyCollectionAndMapIfNull(boolean emptyCollectionAndMapIfNull) {
        this.emptyCollectionAndMapIfNull = emptyCollectionAndMapIfNull;
        return this;
    }

    public PropertyMetaBuilder simpleCodec(SimpleCodec simpleCodec) {
        this.simpleCodec = simpleCodec;
        return this;
    }

    public PropertyMetaBuilder listCodec(ListCodec listCodec) {
        this.listCodec = listCodec;
        return this;
    }

    public PropertyMetaBuilder setCodec(SetCodec setCodec) {
        this.setCodec = setCodec;
        return this;
    }

    public PropertyMetaBuilder mapCodec(MapCodec mapCodec) {
        this.mapCodec = mapCodec;
        return this;
    }
}
