package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.PropertyHelper.allowedTypes;
import static info.archinnov.achilles.entity.metadata.PropertyType.COUNTER_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parser.context.EntityParsingContext;
import info.archinnov.achilles.entity.parser.context.PropertyParsingContext;
import info.archinnov.achilles.entity.parser.validator.PropertyParsingValidator;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.MultiKey;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.JoinColumn;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParser {
    private static final Logger log = LoggerFactory.getLogger(PropertyFilter.class);

    private PropertyHelper propertyHelper = new PropertyHelper();
    private EntityIntrospector entityIntrospector = new EntityIntrospector();
    private PropertyParsingValidator validator = new PropertyParsingValidator();

    public PropertyMeta<?, ?> parse(PropertyParsingContext context) {
        log.debug("Parsing property {} of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Field field = context.getCurrentField();
        inferPropertyNameAndExternalTableName(context);
        context.setCustomConsistencyLevels(propertyHelper.hasConsistencyAnnotation(context.getCurrentField()));

        validator.validateNoDuplicate(context);
        validator.validateWideRowHasNoExternalWideMap(context);

        Class<?> fieldType = field.getType();
        PropertyMeta<?, ?> propertyMeta;
        if (List.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseListProperty(context);
        }

        else if (Set.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseSetProperty(context);
        }

        else if (Map.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseMapProperty(context);
        } else if (Counter.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseCounterProperty(context);
        } else if (WideMap.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseWideMapProperty(context);
        } else {
            propertyMeta = parseSimpleProperty(context);
        }

        if (!context.isPrimaryKey()) {
            context.getPropertyMetas().put(context.getCurrentPropertyName(), propertyMeta);
        }
        return propertyMeta;
    }

    public PropertyMeta<Void, ?> parseSimpleProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as simple property of entity class {}", context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Validator.validateSerializable(field.getType(), "Value of '" + field.getName() + "' should be Serializable");
        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;

        PropertyMeta<Void, ?> propertyMeta = factory(field.getType()) //
                .objectMapper(context.getCurrentObjectMapper()) //
                .type(type) //
                .propertyName(context.getCurrentPropertyName()) //
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()) //
                .accessors(accessors) //
                .consistencyLevels(context.getCurrentConsistencyLevels()) //
                .build();

        log.trace("Built simple property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(), propertyMeta);
        return propertyMeta;
    }

    public PropertyMeta<Void, ?> parseCounterProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as counter property of entity class {}", context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = PropertyType.COUNTER;
        CounterProperties counterProperties = new CounterProperties(context.getCurrentEntityClass()
                .getCanonicalName());

        PropertyMeta<Void, ?> propertyMeta = factory(field.getType()) //
                .objectMapper(context.getCurrentObjectMapper()) //
                .type(type) //
                .propertyName(context.getCurrentPropertyName()) //
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()) //
                .accessors(accessors) //
                .counterProperties(counterProperties) //
                .consistencyLevels(context.getCurrentConsistencyLevels()) //
                .build();

        context.hasSimpleCounterType();
        context.getCounterMetas().add(propertyMeta);
        if (context.isCustomConsistencyLevels()) {
            parseSimpleCounterConsistencyLevel(context, propertyMeta);
        }

        log.trace("Built simple property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(), propertyMeta);
        return propertyMeta;
    }

    public <V> PropertyMeta<Void, V> parseListProperty(PropertyParsingContext context) {

        log.debug("Parsing property {} as list property of entity class {}", context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        Class<V> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClassForListOrSet(genericType, entityClass);

        Validator.validateSerializable(valueClass, "List value type of '" + field.getName()
                + "' should be Serializable");
        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

        PropertyMeta<Void, V> listMeta = factory(valueClass) //
                .objectMapper(context.getCurrentObjectMapper()) //
                .type(type) //
                .propertyName(context.getCurrentPropertyName()) //
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()) //
                .consistencyLevels(context.getCurrentConsistencyLevels()) //
                .accessors(accessors).build();

        log.trace("Built list property meta for property {} of entity class {} : {}", listMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), listMeta);

        return listMeta;

    }

    public <V> PropertyMeta<Void, V> parseSetProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as set property of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        Class<V> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClassForListOrSet(genericType, entityClass);
        Validator.validateSerializable(valueClass, "Set value type of '" + field.getName()
                + "' should be Serializable");
        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

        PropertyMeta<Void, V> setMeta = factory(valueClass) //
                .objectMapper(context.getCurrentObjectMapper()) //
                .type(type) //
                .propertyName(context.getCurrentPropertyName()) //
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()) //
                .consistencyLevels(context.getCurrentConsistencyLevels()) //
                .accessors(accessors)//
                .build();

        log.trace("Built set property meta for property {} of  entity class {} : {}", setMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), setMeta);

        return setMeta;
    }

    @SuppressWarnings("unchecked")
    public <K, V> PropertyMeta<K, V> parseMapProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as map property of entity class {}", context.getCurrentPropertyName(), context
                .getCurrentEntityClass().getCanonicalName());

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();

        validator.validateMapGenerics(field, entityClass);

        Pair<Class<?>, Class<?>> types = determineMapGenericTypes(field);
        Class<K> keyClass = (Class<K>) types.left;
        Class<V> valueClass = (Class<V>) types.right;

        Validator.validateSerializable(valueClass, "Map value type of '" + field.getName()
                + "' should be Serializable");
        Validator.validateSerializable(keyClass, "Map key type of '" + field.getName() + "' should be Serializable");

        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

        PropertyMeta<K, V> mapMeta = factory(keyClass, valueClass) //
                .objectMapper(context.getCurrentObjectMapper()) //
                .type(type) //
                .propertyName(context.getCurrentPropertyName()) //
                .entityClassName(context.getCurrentEntityClass().getCanonicalName()) //
                .consistencyLevels(context.getCurrentConsistencyLevels()) //
                .accessors(accessors).build();

        log.trace("Built map property meta for property {} of entity class {} : {}", mapMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), mapMeta);

        return mapMeta;

    }

    public PropertyMeta<?, ?> parseWideMapProperty(PropertyParsingContext context) {
        log.debug("Parsing property {} as wide map property of entity class {}", context.getCurrentPropertyName(),
                context.getCurrentEntityClass().getCanonicalName());

        validator.validateWideMapGenerics(context);

        Class<?> entityClass = context.getCurrentEntityClass();
        Field field = context.getCurrentField();
        PropertyType type = PropertyType.WIDE_MAP;
        MultiKeyProperties multiKeyProperties = null;
        CounterProperties counterProperties = null;

        Pair<Class<?>, Class<?>> types = determineMapGenericTypes(field);
        Class<?> keyClass = types.left;
        Class<?> valueClass = types.right;
        boolean isCounterValueType = Counter.class.isAssignableFrom(valueClass);

        // Multi or Single Key
        multiKeyProperties = parseWideMapMultiKey(multiKeyProperties, keyClass);

        if (isCounterValueType) {
            counterProperties = new CounterProperties(entityClass.getCanonicalName());
            type = COUNTER_WIDE_MAP;
        }

        Validator.validateSerializable(valueClass, "Wide map value of '" + field.getName()
                + "' should be Serializable");
        Method[] accessors = entityIntrospector.findAccessors(entityClass, field);

        PropertyMeta<?, ?> propertyMeta = factory(keyClass, valueClass) //
                .objectMapper(context.getCurrentObjectMapper()) //
                .type(type) //
                .propertyName(context.getCurrentPropertyName()) //
                .entityClassName(entityClass.getCanonicalName()) //
                .accessors(accessors) //
                .multiKeyProperties(multiKeyProperties) //
                .counterProperties(counterProperties) //
                .consistencyLevels(context.getCurrentConsistencyLevels()) //
                .build();

        if (isCounterValueType) {
            if (propertyHelper.hasConsistencyAnnotation(context.getCurrentField())) {
                throw new AchillesBeanMappingException(
                        "Counter WideMap type '"
                                + entityClass.getCanonicalName()
                                + "' does not support @ConsistencyLevel annotation. Only runtime consistency level is allowed");
            }
            context.getCounterMetas().add(propertyMeta);
        }

        saveWideMapForDeferredBinding(context, propertyMeta);
        fillWideMapCustomConsistencyLevels(context, propertyMeta);

        log.trace("Built wide map property meta for property {} of entity class {} : {}",
                propertyMeta.getPropertyName(), context.getCurrentEntityClass().getCanonicalName(), propertyMeta);
        return propertyMeta;
    }

    public <ID, V> void fillWideMap(EntityParsingContext context, PropertyMeta<Void, ID> idMeta,
            PropertyMeta<?, V> propertyMeta, String externalTableName) {
        log.debug("Filling wide map meta {} of entity class {} with id meta {} info", propertyMeta.getPropertyName(),
                context.getCurrentEntityClass().getCanonicalName(), idMeta.getPropertyName());

        propertyMeta.setIdSerializer(idMeta.getValueSerializer());

        log.trace("Complete wide map property {} of entity class {} : {}", propertyMeta.getPropertyName(), context
                .getCurrentEntityClass().getCanonicalName(), propertyMeta);
    }

    private void inferPropertyNameAndExternalTableName(PropertyParsingContext context) {
        log.trace("Infering property name and column family name for property {}", context.getCurrentPropertyName());

        String propertyName, externalTableName = null;
        Field field = context.getCurrentField();
        if (context.isJoinColumn()) {
            JoinColumn column = field.getAnnotation(JoinColumn.class);
            externalTableName = field.getAnnotation(JoinColumn.class).table();
            propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
        } else if (context.isPrimaryKey()) {
            propertyName = field.getName();
        } else {
            Column column = field.getAnnotation(Column.class);
            externalTableName = field.getAnnotation(Column.class).table();
            propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
        }

        context.setCurrentPropertyName(propertyName);
        context.setCurrentExternalTableName(externalTableName);

    }

    private Pair<Class<?>, Class<?>> determineMapGenericTypes(Field field) {
        log.trace("Determine generic types for field Map<K,V> {} of entity class {}", field.getName(), field
                .getDeclaringClass().getCanonicalName());

        Type genericType = field.getGenericType();
        ParameterizedType pt = (ParameterizedType) genericType;
        Type[] actualTypeArguments = pt.getActualTypeArguments();

        return new Pair<Class<?>, Class<?>>((Class<?>) actualTypeArguments[0], (Class<?>) actualTypeArguments[1]);
    }

    private MultiKeyProperties parseWideMapMultiKey(MultiKeyProperties multiKeyProperties, Class<?> keyClass) {
        log.trace("Parsing wide map multi key class", keyClass.getCanonicalName());

        if (MultiKey.class.isAssignableFrom(keyClass)) {
            multiKeyProperties = propertyHelper.parseMultiKey(keyClass);
        } else {
            PropertyParsingValidator.validateAllowedTypes(keyClass, allowedTypes,
                    "The class '" + keyClass.getCanonicalName()
                            + "' is not allowed as WideMap key. Did you forget to implement MultiKey interface ?");
        }

        log.trace("Built multi key properties", multiKeyProperties);
        return multiKeyProperties;
    }

    private void saveWideMapForDeferredBinding(PropertyParsingContext context, PropertyMeta<?, ?> propertyMeta) {
        log.trace("Saving wide map meta {} for deferred binding", propertyMeta);

        String externalCFName;

        Class<?> entityClass = context.getCurrentEntityClass();
        if (context.isColumnFamilyDirectMapping()) {
            externalCFName = context.getCurrentColumnFamilyName();
            Validator.validateBeanMappingTrue(
                    StringUtils.isBlank(context.getCurrentExternalTableName()),
                    "External Column Family should be defined for counter WideMap property '"
                            + propertyMeta.getPropertyName() + "' of entity '" + entityClass.getCanonicalName()
                            + "'. Did you forget to add 'table' attribute to @Column/@JoinColumn annotation ?");
        } else {
            externalCFName = context.getCurrentExternalTableName();
            Validator.validateBeanMappingFalse(
                    StringUtils.isBlank(externalCFName),
                    "External Column Family should be defined for WideMap property '"
                            + propertyMeta.getPropertyName() + "' of entity '" + entityClass.getCanonicalName()
                            + "'. Did you forget to add 'table' attribute to @Column/@JoinColumn annotation ?");

        }
        propertyMeta.setExternalCfName(externalCFName);
        if (context.isJoinColumn()) {
            context.getJoinWideMaps().put(propertyMeta, externalCFName);
        } else {
            context.getWideMaps().put(propertyMeta, externalCFName);
        }

    }

    private void fillWideMapCustomConsistencyLevels(PropertyParsingContext context, PropertyMeta<?, ?> propertyMeta) {
        log.trace("Determining wide map meta {} custom consistency levels", propertyMeta);
        boolean isCustomConsistencyLevel = propertyHelper.hasConsistencyAnnotation(context.getCurrentField());
        String externalTableName = context.getCurrentExternalTableName();

        if (isCustomConsistencyLevel) {
            Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = propertyHelper.findConsistencyLevels(
                    context.getCurrentField(), context.getConfigurableCLPolicy());

            context.getConfigurableCLPolicy().setConsistencyLevelForRead(consistencyLevels.left.getHectorLevel(),
                    externalTableName);
            context.getConfigurableCLPolicy().setConsistencyLevelForWrite(consistencyLevels.right.getHectorLevel(),
                    externalTableName);

            propertyMeta.setConsistencyLevels(consistencyLevels);

            log.trace("Found custom consistency levels : {}", consistencyLevels);
        }
    }

    private void parseSimpleCounterConsistencyLevel(PropertyParsingContext context, PropertyMeta<?, ?> propertyMeta) {

        log.trace("Parse custom consistency levels for counter property {}", propertyMeta);
        Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = propertyHelper.findConsistencyLevels(
                context.getCurrentField(), context.getConfigurableCLPolicy());

        validator.validateConsistencyLevelForCounter(context, consistencyLevels);

        log.trace("Found custom consistency levels : {}", consistencyLevels);
        propertyMeta.setConsistencyLevels(consistencyLevels);
    }

}
