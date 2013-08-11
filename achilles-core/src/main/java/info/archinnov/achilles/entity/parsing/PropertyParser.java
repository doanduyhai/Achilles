package info.archinnov.achilles.entity.parsing;

import static info.archinnov.achilles.entity.metadata.PropertyMetaBuilder.factory;
import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.entity.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.entity.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.helper.PropertyHelper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.JoinColumn;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParser
{
    private static final Logger log = LoggerFactory.getLogger(PropertyFilter.class);

    private PropertyHelper propertyHelper = new PropertyHelper();
    private CompoundKeyParser compoundKeyParser = new CompoundKeyParser();
    private EntityIntrospector entityIntrospector = new EntityIntrospector();
    private PropertyParsingValidator validator = new PropertyParsingValidator();

    public PropertyMeta parse(PropertyParsingContext context)
    {
        log.debug("Parsing property {} of entity class {}", context.getCurrentPropertyName(),
                context
                        .getCurrentEntityClass().getCanonicalName());

        Field field = context.getCurrentField();
        inferPropertyNameAndExternalTableName(context);
        context.setCustomConsistencyLevels(propertyHelper.hasConsistencyAnnotation(context
                .getCurrentField()));

        validator.validateNoDuplicate(context);

        Class<?> fieldType = field.getType();
        PropertyMeta propertyMeta;

        if (List.class.isAssignableFrom(fieldType))
        {
            propertyMeta = parseListProperty(context);
        }
        else if (Set.class.isAssignableFrom(fieldType))
        {
            propertyMeta = parseSetProperty(context);
        }
        else if (Map.class.isAssignableFrom(fieldType))
        {
            propertyMeta = parseMapProperty(context);
        }
        else if (Counter.class.isAssignableFrom(fieldType))
        {
            propertyMeta = parseCounterProperty(context);
        }
        else if (context.isEmbeddedId())
        {
            propertyMeta = parseEmbeddedId(context);
        }
        else if (context.isPrimaryKey())
        {
            propertyMeta = parseSimpleProperty(context);
            propertyMeta.setType(ID);
        }
        else
        {
            propertyMeta = parseSimpleProperty(context);
        }
        context.getPropertyMetas().put(context.getCurrentPropertyName(), propertyMeta);
        return propertyMeta;
    }

    protected PropertyMeta parseEmbeddedId(PropertyParsingContext context)
    {
        log.debug("Parsing property {} as embedded id of entity class {}",
                context.getCurrentPropertyName(), context
                        .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = EMBEDDED_ID;

        EmbeddedIdProperties embeddedIdProperties = parseCompoundKey(field.getType());
        PropertyMeta propertyMeta = factory()
                .objectMapper(context.getCurrentObjectMapper())
                .type(type)
                .propertyName(context.getCurrentPropertyName())
                .embeddedIdProperties(embeddedIdProperties)
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .accessors(accessors)
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .build(Void.class, field.getType());

        log.trace("Built embedded id property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(),
                propertyMeta);
        return propertyMeta;
    }

    protected PropertyMeta parseSimpleProperty(PropertyParsingContext context)
    {
        log.debug("Parsing property {} as simple property of entity class {}",
                context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;

        PropertyMeta propertyMeta = factory()
                .objectMapper(context.getCurrentObjectMapper())
                .type(type)
                .propertyName(context.getCurrentPropertyName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .accessors(accessors)
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .build(Void.class, field.getType());

        log.trace("Built simple property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(),
                propertyMeta);
        return propertyMeta;
    }

    protected PropertyMeta parseCounterProperty(PropertyParsingContext context)
    {
        log.debug("Parsing property {} as counter property of entity class {}",
                context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);

        PropertyType type = PropertyType.COUNTER;

        CounterProperties counterProperties = new CounterProperties(context.getCurrentEntityClass()
                .getCanonicalName());

        PropertyMeta propertyMeta = factory()
                .objectMapper(context.getCurrentObjectMapper())
                .type(type)
                .propertyName(context.getCurrentPropertyName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .accessors(accessors)
                .counterProperties(counterProperties)
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .build(Void.class, field.getType());

        context.hasSimpleCounterType();
        context.getCounterMetas().add(propertyMeta);
        if (context.isCustomConsistencyLevels())
        {
            parseSimpleCounterConsistencyLevel(context, propertyMeta);
        }

        log.trace("Built simple property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(),
                propertyMeta);
        return propertyMeta;
    }

    public <V> PropertyMeta parseListProperty(PropertyParsingContext context)
    {

        log.debug("Parsing property {} as list property of entity class {}",
                context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        Class<V> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClassForListOrSet(genericType, entityClass);

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

        PropertyMeta listMeta = factory()
                //
                .objectMapper(context.getCurrentObjectMapper())
                .type(type)
                .propertyName(context.getCurrentPropertyName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .accessors(accessors)
                .build(Void.class, valueClass);

        log.trace("Built list property meta for property {} of entity class {} : {}",
                listMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), listMeta);

        return listMeta;

    }

    public <V> PropertyMeta parseSetProperty(PropertyParsingContext context)
    {
        log.debug("Parsing property {} as set property of entity class {}",
                context.getCurrentPropertyName(), context
                        .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Class<V> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClassForListOrSet(genericType, entityClass);
        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

        PropertyMeta setMeta = factory()
                //
                .objectMapper(context.getCurrentObjectMapper())
                .type(type)
                .propertyName(context.getCurrentPropertyName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .accessors(accessors)
                .build(Void.class, valueClass);

        log.trace("Built set property meta for property {} of  entity class {} : {}",
                setMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), setMeta);

        return setMeta;
    }

    protected <K, V> PropertyMeta parseMapProperty(PropertyParsingContext context)
    {
        log.debug("Parsing property {} as map property of entity class {}",
                context.getCurrentPropertyName(), context
                        .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        validator.validateMapGenerics(field, entityClass);

        Pair<Class<K>, Class<V>> types = determineMapGenericTypes(field);
        Class<K> keyClass = types.left;
        Class<V> valueClass = types.right;

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

        PropertyMeta mapMeta = factory()
                .objectMapper(context.getCurrentObjectMapper())
                .type(type)
                .propertyName(context.getCurrentPropertyName())
                .entityClassName(context.getCurrentEntityClass().getCanonicalName())
                .consistencyLevels(context.getCurrentConsistencyLevels())
                .accessors(accessors)
                .build(keyClass, valueClass);

        log.trace("Built map property meta for property {} of entity class {} : {}",
                mapMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), mapMeta);

        return mapMeta;

    }

    public void fillWideMap(EntityParsingContext context, PropertyMeta idMeta,
            PropertyMeta propertyMeta,
            String externalTableName)
    {
        log.debug("Filling wide map meta {} of entity class {} with id meta {} info",
                propertyMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), idMeta.getPropertyName());

        propertyMeta.setIdClass(idMeta.getValueClass());

        log.trace("Complete wide map property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context
                        .getCurrentEntityClass().getCanonicalName(), propertyMeta);
    }

    private void inferPropertyNameAndExternalTableName(PropertyParsingContext context)
    {
        log.trace("Infering property name and column family name for property {}",
                context.getCurrentPropertyName());

        String propertyName, externalTableName = null;
        Field field = context.getCurrentField();
        if (context.isJoinColumn())
        {
            JoinColumn column = field.getAnnotation(JoinColumn.class);
            externalTableName = field.getAnnotation(JoinColumn.class).table();
            propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
        }
        else if (context.isPrimaryKey())
        {
            propertyName = field.getName();
        }
        else
        {
            Column column = field.getAnnotation(Column.class);
            externalTableName = field.getAnnotation(Column.class).table();
            propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
        }

        context.setCurrentPropertyName(propertyName);
        context.setCurrentExternalTableName(externalTableName);

    }

    private <K, V> Pair<Class<K>, Class<V>> determineMapGenericTypes(Field field)
    {
        log.trace("Determine generic types for field Map<K,V> {} of entity class {}",
                field.getName(), field
                        .getDeclaringClass().getCanonicalName());

        Type genericType = field.getGenericType();
        ParameterizedType pt = (ParameterizedType) genericType;
        Type[] actualTypeArguments = pt.getActualTypeArguments();

        Class<K> keyClass = propertyHelper.getClassFromType(actualTypeArguments[0]);
        Class<V> valueClass = propertyHelper.getClassFromType(actualTypeArguments[1]);

        return Pair.create(keyClass, valueClass);
    }

    private EmbeddedIdProperties parseCompoundKey(Class<?> keyClass)
    {
        log.trace("Parsing compound key class", keyClass.getCanonicalName());
        EmbeddedIdProperties embeddedIdProperties = null;

        embeddedIdProperties = compoundKeyParser.parseEmbeddedId(keyClass);

        log.trace("Built compound key properties", embeddedIdProperties);
        return embeddedIdProperties;
    }

    private void parseSimpleCounterConsistencyLevel(PropertyParsingContext context,
            PropertyMeta propertyMeta)
    {

        log.trace("Parse custom consistency levels for counter property {}", propertyMeta);
        Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = propertyHelper
                .findConsistencyLevels(
                        context.getCurrentField(), context.getConfigurableCLPolicy());

        validator.validateConsistencyLevelForCounter(context, consistencyLevels);

        log.trace("Found custom consistency levels : {}", consistencyLevels);
        propertyMeta.setConsistencyLevels(consistencyLevels);
    }

}
