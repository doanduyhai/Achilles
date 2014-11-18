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
package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaBuilder.factory;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.ID;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.LIST;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.MAP;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SET;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.validation.constraints.NotNull;

import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.EmptyCollectionIfNull;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.TimeUUID;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.metadata.holder.CounterProperties;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdProperties;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;

public class PropertyParser {
    private static final Logger log = LoggerFactory.getLogger(PropertyFilter.class);

    public static Set<Class<?>> allowedTypes = new HashSet<>();

    static {
        // Bytes
        allowedTypes.add(byte.class);
        allowedTypes.add(Byte.class);
        allowedTypes.add(byte[].class);
        allowedTypes.add(ByteBuffer.class);

        // Boolean
        allowedTypes.add(Boolean.class);
        allowedTypes.add(boolean.class);

        // Date
        allowedTypes.add(Date.class);

        // Double
        allowedTypes.add(Double.class);
        allowedTypes.add(double.class);

        // Float
        allowedTypes.add(BigDecimal.class);
        allowedTypes.add(Float.class);
        allowedTypes.add(float.class);

        // InetAddress
        allowedTypes.add(InetAddress.class);

        // Integer
        allowedTypes.add(BigInteger.class);
        allowedTypes.add(Integer.class);
        allowedTypes.add(int.class);

        // Long
        allowedTypes.add(Long.class);
        allowedTypes.add(long.class);

        // String
        allowedTypes.add(String.class);

        // UUID
        allowedTypes.add(UUID.class);
    }

    private EntityIntrospector entityIntrospector = EntityIntrospector.Singleton.INSTANCE.get();
    private PropertyParsingValidator validator = PropertyParsingValidator.Singleton.INSTANCE.get();
    private PropertyFilter filter = PropertyFilter.Singleton.INSTANCE.get();
    private CodecFactory codecFactory = CodecFactory.Singleton.INSTANCE.get();

    public static String getIndexName(Field field) {
        log.debug("Check @Index annotation on field {} of class {}", field.getName(), field.getDeclaringClass().getCanonicalName());
        String indexName = null;
        Index index = field.getAnnotation(Index.class);
        if (index != null) {
            indexName = index.name();
        }
        return indexName;
    }

    public static <T> boolean isSupportedNativeType(Class<T> valueClass) {
        return valueClass != null && (allowedTypes.contains(valueClass) || (ByteBuffer.class.isAssignableFrom(valueClass)));
    }

    public static <T> boolean isAssignableFromNativeType(Class<T> valueClass) {
        for (Class<?> allowedType : allowedTypes) {
            if (allowedType.isAssignableFrom(valueClass)) {
                return true;
            }
        }
        return false;
    }

    public static <T> boolean isSupportedType(Class<T> valueClass) {
        return isSupportedNativeType(valueClass) || (valueClass != null && valueClass.isEnum());
    }


    public Pair<ConsistencyLevel, ConsistencyLevel> findConsistencyLevels(Field field, Pair<ConsistencyLevel, ConsistencyLevel> defaultConsistencyLevels) {
        log.debug("Find consistency configuration for field {} of class {}", field.getName(), field.getDeclaringClass()
                .getCanonicalName());

        Consistency clevel = field.getAnnotation(Consistency.class);

        ConsistencyLevel defaultGlobalRead = defaultConsistencyLevels.left;
        ConsistencyLevel defaultGlobalWrite = defaultConsistencyLevels.right;

        if (clevel != null) {
            defaultGlobalRead = clevel.read();
            defaultGlobalWrite = clevel.write();
        }

        log.trace("Found consistency levels : {} / {}", defaultGlobalRead, defaultGlobalWrite);
        return Pair.create(defaultGlobalRead, defaultGlobalWrite);
    }

    public Class<?> inferEntityClassFromInterceptor(Interceptor<?> interceptor) {
        for (Type type : interceptor.getClass().getGenericInterfaces()) {
            if (type instanceof ParameterizedType) {
                final ParameterizedType parameterizedType = (ParameterizedType) type;
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                return TypeParser.getClassFromType(actualTypeArguments[0]);
            }
        }
        return null;
    }

    public PropertyMeta parse(PropertyParsingContext context) {
        log.debug("Parsing property {} of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Field field = context.getCurrentField();
        context.setCustomConsistencyLevels(filter.hasAnnotation(context.getCurrentField(), Consistency.class));

        validator.validateNoDuplicatePropertyName(context);
        validator.validateIndexIfSet(context);

        Class<?> fieldType = field.getType();
        PropertyMeta propertyMeta;

        if (List.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseListProperty(context);
        } else if (Set.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseSetProperty(context);
        } else if (Map.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseMapProperty(context);
        } else if (Counter.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseCounterProperty(context);
        } else if (context.isEmbeddedId()) {
            propertyMeta = parseEmbeddedId(context);
        } else if (context.isPrimaryKey()) {
            propertyMeta = parseId(context);
        } else {
            propertyMeta = parseSimpleProperty(context);
            String indexName = getIndexName(field);
            if (indexName != null) {
                propertyMeta.setIndexProperties(new IndexProperties(indexName, propertyMeta.getCQL3ColumnName()));
            }
        }
        context.getPropertyMetas().put(context.getCurrentPropertyName(), propertyMeta);

        validator.validateNoDuplicateCQLName(context);
        return propertyMeta;
    }

    protected PropertyMeta parseId(PropertyParsingContext context) {
        log.debug("Parsing property {} as id of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        PropertyMeta idMeta = parseSimpleProperty(context);
        idMeta.setType(ID);
        Id id = context.getCurrentField().getAnnotation(Id.class);
        String propertyName = StringUtils.isNotBlank(id.name()) ? id.name() : context.getCurrentPropertyName();
        idMeta.setPropertyName(propertyName);

        return idMeta;
    }

    protected PropertyMeta parseEmbeddedId(PropertyParsingContext context) {
        log.debug("Parsing property {} as embedded id of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        final Class<?> embeddedIdClass = field.getType();

        Validator.validateInstantiable(embeddedIdClass);

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = EMBEDDED_ID;

        EmbeddedIdProperties embeddedIdProperties = extractEmbeddedIdProperties(embeddedIdClass, context);
        PropertyMeta propertyMeta = factory().objectMapper(context.getCurrentObjectMapper()).type(type)
                .propertyName(context.getCurrentPropertyName()).embeddedIdProperties(embeddedIdProperties)
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()).accessors(accessors).field(field)
                .consistencyLevels(context.getCurrentConsistencyLevels()).build(Void.class, embeddedIdClass);

        log.trace("Built embedded id property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(), propertyMeta);
        return propertyMeta;
    }

    protected PropertyMeta parseSimpleProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as simple property of entity class {}", context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        final boolean staticColumn = isStaticColumn(field);
        boolean timeUUID = isTimeUUID(context, field);

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        final Codec simpleCodec = codecFactory.parseSimpleField(context);
        final Class<?> cql3ValueType = codecFactory.determineCQL3ValueType(simpleCodec, timeUUID);
        PropertyType type = SIMPLE;

        PropertyMeta propertyMeta = factory().objectMapper(context.getCurrentObjectMapper()).type(type)
                .propertyName(context.getCurrentPropertyName()).cqlColumnName(context.getCurrentCQL3ColumnName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()).accessors(accessors)
                .consistencyLevels(context.getCurrentConsistencyLevels()).field(field).timeuuid(timeUUID)
                .staticColumn(staticColumn).simpleCodec(simpleCodec).cql3ValueClass(cql3ValueType)
                .build(Void.class, field.getType());

        log.trace("Built simple property meta for property {} of entity class {} : {}", propertyMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), propertyMeta);
        return propertyMeta;
    }

    protected PropertyMeta parseCounterProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as counter property of entity class {}", context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        final boolean staticColumn = isStaticColumn(field);
        PropertyType type = PropertyType.COUNTER;

        CounterProperties counterProperties = new CounterProperties(context.getCurrentEntityClass().getCanonicalName());

        PropertyMeta propertyMeta = factory().objectMapper(context.getCurrentObjectMapper()).type(type)
                .propertyName(context.getCurrentPropertyName()).cqlColumnName(context.getCurrentCQL3ColumnName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()).accessors(accessors).field(field)
                .counterProperties(counterProperties).consistencyLevels(context.getCurrentConsistencyLevels()).staticColumn(staticColumn)
                .cql3ValueClass(Long.class)
                .build(Void.class, field.getType());

        context.hasSimpleCounterType();
        context.getCounterMetas().add(propertyMeta);
        if (context.isCustomConsistencyLevels()) {
            parseSimpleCounterConsistencyLevel(context, propertyMeta);
        }

        log.trace("Built simple property meta for property {} of entity class {} : {}", propertyMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), propertyMeta);
        return propertyMeta;
    }

    public <V> PropertyMeta parseListProperty(PropertyParsingContext context) {

        log.debug("Parsing property {} as list property of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        final boolean timeUUID = isTimeUUID(context, field);
        final boolean emptyCollectionIfNull = mapNullCollectionAndMapToEmpty(field);
        final boolean staticColumn = isStaticColumn(field);
        Class<V> valueClass;
        Type genericType = field.getGenericType();
        valueClass = TypeParser.inferValueClassForListOrSet(genericType, entityClass);

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        final ListCodec listCodec = codecFactory.parseListField(context);
        final Class<?> cql3ValueType = codecFactory.determineCQL3ValueType(listCodec, timeUUID);

        PropertyType type = LIST;

        PropertyMeta listMeta = factory().objectMapper(context.getCurrentObjectMapper()).type(type)
                .propertyName(context.getCurrentPropertyName()).cqlColumnName(context.getCurrentCQL3ColumnName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .consistencyLevels(context.getCurrentConsistencyLevels()).accessors(accessors).field(field)
                .timeuuid(timeUUID).emptyCollectionAndMapIfNull(emptyCollectionIfNull).staticColumn(staticColumn)
                .listCodec(listCodec).cql3ValueClass(cql3ValueType)
                .build(Void.class, valueClass);

        log.trace("Built list property meta for property {} of entity class {} : {}", listMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), listMeta);

        return listMeta;

    }

    public <V> PropertyMeta parseSetProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as set property of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        final boolean timeUUID = isTimeUUID(context, field);
        final boolean emptyCollectionIfNull = mapNullCollectionAndMapToEmpty(field);
        final boolean staticColumn = isStaticColumn(field);
        Class<V> valueClass;
        Type genericType = field.getGenericType();

        valueClass = TypeParser.inferValueClassForListOrSet(genericType, entityClass);
        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        final SetCodec setCodec = codecFactory.parseSetField(context);
        final Class<?> cql3ValueType = codecFactory.determineCQL3ValueType(setCodec, timeUUID);

        PropertyType type = SET;

        PropertyMeta setMeta = factory().objectMapper(context.getCurrentObjectMapper()).type(type)
                .propertyName(context.getCurrentPropertyName()).cqlColumnName(context.getCurrentCQL3ColumnName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .consistencyLevels(context.getCurrentConsistencyLevels()).accessors(accessors).field(field)
                .timeuuid(timeUUID).emptyCollectionAndMapIfNull(emptyCollectionIfNull).staticColumn(staticColumn)
                .setCodec(setCodec).cql3ValueClass(cql3ValueType)
                .build(Void.class, valueClass);

        log.trace("Built set property meta for property {} of  entity class {} : {}", setMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), setMeta);

        return setMeta;
    }

    protected <K, V> PropertyMeta parseMapProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as map property of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        final boolean timeUUID = isTimeUUID(context, field);
        final boolean emptyCollectionIfNull = mapNullCollectionAndMapToEmpty(field);
        final boolean staticColumn = isStaticColumn(field);
        validator.validateMapGenerics(field, entityClass);

        Pair<Class<K>, Class<V>> types = TypeParser.determineMapGenericTypes(field);
        Class<K> keyClass = types.left;
        Class<V> valueClass = types.right;

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        final MapCodec mapCodec = codecFactory.parseMapField(context);
        final Class<?> cql3KeyType = codecFactory.determineCQL3KeyType(mapCodec, timeUUID);
        final Class<?> cql3ValueType = codecFactory.determineCQL3ValueType(mapCodec, timeUUID);

        PropertyType type = MAP;

        PropertyMeta mapMeta = factory().objectMapper(context.getCurrentObjectMapper()).type(type)
                .propertyName(context.getCurrentPropertyName()).cqlColumnName(context.getCurrentCQL3ColumnName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .consistencyLevels(context.getCurrentConsistencyLevels()).accessors(accessors).field(field)
                .timeuuid(timeUUID).emptyCollectionAndMapIfNull(emptyCollectionIfNull).staticColumn(staticColumn)
                .mapCodec(mapCodec).cql3KeyClass(cql3KeyType).cql3ValueClass(cql3ValueType)
                .build(keyClass, valueClass);

        log.trace("Built map property meta for property {} of entity class {} : {}", mapMeta.getPropertyName(), context
                .getCurrentEntityClass().getCanonicalName(), mapMeta);

        return mapMeta;

    }

    private EmbeddedIdProperties extractEmbeddedIdProperties(Class<?> keyClass, PropertyParsingContext context) {
        log.trace("Parsing compound key class", keyClass.getCanonicalName());
        EmbeddedIdProperties embeddedIdProperties = new EmbeddedIdParser(context).parseEmbeddedId(keyClass, this);
        log.trace("Built compound key properties", embeddedIdProperties);
        return embeddedIdProperties;
    }

    private void parseSimpleCounterConsistencyLevel(PropertyParsingContext context, PropertyMeta propertyMeta) {

        log.trace("Parse custom consistency levels for counter property {}", propertyMeta);
        Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = findConsistencyLevels(context.getCurrentField(), context.getCurrentConsistencyLevels());

        validator.validateConsistencyLevelForCounter(context, consistencyLevels);

        log.trace("Found custom consistency levels : {}", consistencyLevels);
        propertyMeta.setConsistencyLevels(consistencyLevels);
    }

    private boolean isTimeUUID(PropertyParsingContext context, Field field) {
        boolean timeUUID = false;
        if (filter.hasAnnotation(field, TimeUUID.class)) {
            Validator.validateBeanMappingTrue(field.getType().equals(UUID.class),
                    "The field '%s' from class '%s' annotated with @TimeUUID should be of java.util.UUID type",
                    field.getName(), context.getCurrentEntityClass().getCanonicalName());
            timeUUID = true;
        }
        return timeUUID;
    }

    private boolean mapNullCollectionAndMapToEmpty(Field field) {
        return filter.hasAnnotation(field, EmptyCollectionIfNull.class) || filter.hasAnnotation(field, NotNull.class);
    }

    private boolean isStaticColumn(Field field) {
        Column column = field.getAnnotation(Column.class);
        return column!= null && column.staticColumn();
    }

    public static enum Singleton {
        INSTANCE;

        private final PropertyParser instance = new PropertyParser();

        public PropertyParser get() {
            return instance;
        }
    }
}
