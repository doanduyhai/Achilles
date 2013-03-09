package info.archinnov.achilles.entity.parser;

import static info.archinnov.achilles.entity.PropertyHelper.allowedCounterTypes;
import static info.archinnov.achilles.entity.PropertyHelper.allowedTypes;
import static info.archinnov.achilles.entity.PropertyHelper.isSupportedType;
import static info.archinnov.achilles.entity.manager.ThriftEntityManagerFactoryImpl.counterDaoTL;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP_COUNTER;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import static info.archinnov.achilles.entity.parser.EntityParser.counterMetasTL;
import static info.archinnov.achilles.entity.parser.EntityParser.entityClassTL;
import static info.archinnov.achilles.entity.parser.EntityParser.externalWideMapTL;
import static info.archinnov.achilles.entity.parser.EntityParser.objectMapperTL;
import static info.archinnov.achilles.entity.parser.EntityParser.propertyMetasTL;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.PropertyHelper;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.MultiKey;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.exception.BeanMappingException;
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
import me.prettyprint.hector.api.Keyspace;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * PropertyParser
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyParser {

    private PropertyHelper propertyHelper = new PropertyHelper();
    private EntityHelper entityHelper = new EntityHelper();

    public PropertyMeta<?, ?> parse(Field field, boolean joinColumn) {

        Class<?> entityClass = entityClassTL.get();

        String externalTableName;
        String propertyName;

        if (joinColumn) {
            JoinColumn column = field.getAnnotation(JoinColumn.class);
            externalTableName = field.getAnnotation(JoinColumn.class).table();
            propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
        } else {
            Column column = field.getAnnotation(Column.class);
            externalTableName = field.getAnnotation(Column.class).table();
            propertyName = StringUtils.isNotBlank(column.name()) ? column.name() : field.getName();
        }
        boolean isCounter = propertyHelper.hasCounterAnnotation(field);
        boolean isExternal = StringUtils.isNotBlank(externalTableName);

        Validator.validateFalse(propertyMetasTL.get().containsKey(propertyName), "The property '" + propertyName
                + "' is already used for the entity '" + entityClass.getCanonicalName() + "'");

        PropertyMeta<?, ?> propertyMeta = null;
        if (isExternal && isCounter) {
            throw new BeanMappingException(
                    "Error for field '"
                            + field.getName()
                            + "' of entity '"
                            + entityClass.getCanonicalName()
                            + "'. Counter value are already stored in external column families. There is no sense having a counter with external table");
        } else {
            Class<?> fieldType = field.getType();

            if (List.class.isAssignableFrom(fieldType)) {
                propertyMeta = parseListProperty(field, propertyName);
            }

            else if (Set.class.isAssignableFrom(fieldType)) {
                propertyMeta = parseSetProperty(field, propertyName);
            }

            else if (Map.class.isAssignableFrom(fieldType)) {
                propertyMeta = parseMapProperty(field, propertyName);
            }

            else if (WideMap.class.isAssignableFrom(fieldType)) {
                propertyMeta = parseWideMapProperty(field, propertyName, entityClass.getCanonicalName());
            }

            else {
                propertyMeta = parseSimpleProperty(field, propertyName, entityClass.getCanonicalName());
            }

            propertyMetasTL.get().put(propertyName, propertyMeta);
            if (isExternal) {
                externalWideMapTL.get().put(propertyMeta, externalTableName);
            }
        }

        return propertyMeta;
    }

    public PropertyMeta<Void, ?> parseSimpleProperty(Field field, String propertyName, String fqcn) {
        Validator.validateSerializable(field.getType(), "Value of '" + field.getName() + "' should be Serializable");
        ObjectMapper objectMapper = objectMapperTL.get();
        Class<?> entityClass = entityClassTL.get();
        Method[] accessors = entityHelper.findAccessors(entityClass, field);

        PropertyType type;
        CounterProperties counterProperties = null;
        if (propertyHelper.hasCounterAnnotation(field)) {
            Validator.validateAllowedTypes(field.getType(), allowedCounterTypes, "Wrong counter type for the field '"
                    + field.getName() + "'. Only java.lang.Long and primitive long are allowed for @Counter types");
            type = PropertyType.COUNTER;
            counterProperties = new CounterProperties(fqcn, counterDaoTL.get());

        } else {
            type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;
        }

        PropertyMeta<Void, ?> propertyMeta = factory(field.getType()) //
                .objectMapper(objectMapper) //
                .type(type) //
                .propertyName(propertyName) //
                .accessors(accessors) //
                .counterProperties(counterProperties) //
                .build();

        if (counterProperties != null) {
            counterMetasTL.get().add(propertyMeta);
        }

        return propertyMeta;

    }

    public PropertyMeta<Void, ?> parseListProperty(Field field, String propertyName) {

        ObjectMapper objectMapper = objectMapperTL.get();
        Class<?> entityClass = entityClassTL.get();

        Class<?> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClass(genericType);

        Validator.validateSerializable(valueClass, "List value type of '" + field.getName()
                + "' should be Serializable");
        Method[] accessors = entityHelper.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

        return factory(valueClass) //
                .objectMapper(objectMapper) //
                .type(type) //
                .propertyName(propertyName) //
                .accessors(accessors).build();

    }

    public PropertyMeta<Void, ?> parseSetProperty(Field field, String propertyName) {
        ObjectMapper objectMapper = objectMapperTL.get();
        Class<?> entityClass = entityClassTL.get();

        Class<?> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClass(genericType);
        Validator.validateSerializable(valueClass, "Set value type of '" + field.getName()
                + "' should be Serializable");
        Method[] accessors = entityHelper.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

        return factory(valueClass) //
                .objectMapper(objectMapper) //
                .type(type) //
                .propertyName(propertyName) //
                .accessors(accessors).build();
    }

    public PropertyMeta<?, ?> parseMapProperty(Field field, String propertyName) {

        ObjectMapper objectMapper = objectMapperTL.get();
        Class<?> entityClass = entityClassTL.get();

        Class<?> valueClass;
        Class<?> keyType;

        Type genericType = field.getGenericType();

        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 1) {
                keyType = (Class<?>) actualTypeArguments[0];
                valueClass = (Class<?>) actualTypeArguments[1];
            } else {
                keyType = Object.class;
                valueClass = Object.class;
            }
        } else {
            keyType = Object.class;
            valueClass = Object.class;
        }
        Validator.validateSerializable(valueClass, "Map value type of '" + field.getName()
                + "' should be Serializable");
        Validator.validateSerializable(keyType, "Map key type of '" + field.getName() + "' should be Serializable");
        Method[] accessors = entityHelper.findAccessors(entityClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

        return factory(keyType, valueClass) //
                .objectMapper(objectMapper) //
                .type(type) //
                .propertyName(propertyName) //
                .accessors(accessors).build();

    }

    public PropertyMeta<?, ?> parseWideMapProperty(Field field, String propertyName, String fqcn) {

        ObjectMapper objectMapper = objectMapperTL.get();
        Class<?> entityClass = entityClassTL.get();

        PropertyType type = PropertyType.WIDE_MAP;
        MultiKeyProperties multiKeyProperties = null;
        Class<?> keyClass = null;
        Class<?> valueClass = null;

        CounterProperties counterProperties = null;
        // Multi or Single Key
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 1) {
                keyClass = (Class<?>) actualTypeArguments[0];
                valueClass = (Class<?>) actualTypeArguments[1];

                if (MultiKey.class.isAssignableFrom(keyClass)) {
                    multiKeyProperties = propertyHelper.parseMultiKey(keyClass);
                } else {
                    if (propertyHelper.hasCounterAnnotation(field)) {
                        Validator.validateAllowedTypes(valueClass, allowedCounterTypes,
                                "Wrong counter type for the field '" + field.getName()
                                        + "'. Only java.lang.Long and primitive long are allowed for @Counter types");
                        type = EXTERNAL_WIDE_MAP_COUNTER;
                        counterProperties = new CounterProperties(fqcn, counterDaoTL.get());
                    } else {

                        Validator
                                .validateAllowedTypes(
                                        keyClass,
                                        allowedTypes,
                                        "The class '"
                                                + keyClass.getCanonicalName()
                                                + "' is not allowed as WideMap key. Did you forget to implement MultiKey interface ?");
                    }
                }
            } else {
                throw new BeanMappingException("The WideMap type should be parameterized with <K,V> for the entity "
                        + entityClass.getCanonicalName());
            }
        } else {
            throw new BeanMappingException("The WideMap type should be parameterized for the entity "
                    + entityClass.getCanonicalName());
        }

        Validator.validateSerializable(valueClass, "Wide map value of '" + field.getName()
                + "' should be Serializable");
        Method[] accessors = entityHelper.findAccessors(entityClass, field);

        PropertyMeta<?, ?> propertyMeta = factory(keyClass, valueClass) //
                .objectMapper(objectMapper) //
                .type(type) //
                .propertyName(propertyName) //
                .accessors(accessors) //
                .multiKeyProperties(multiKeyProperties) //
                .counterProperties(counterProperties) //
                .build();

        if (counterProperties != null) {
            counterMetasTL.get().add(propertyMeta);
        }

        return propertyMeta;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <ID> void fillExternalWideMap(Keyspace keyspace, PropertyMeta<Void, ID> idMeta,
            PropertyMeta<?, ?> propertyMeta, String externalTableName) {
        propertyMeta.setType(EXTERNAL_WIDE_MAP);
        GenericCompositeDao<ID, ?> dao;
        if (isSupportedType(propertyMeta.getValueClass())) {
            dao = new GenericCompositeDao(keyspace, idMeta.getValueSerializer(), propertyMeta.getValueSerializer(),
                    externalTableName);
        } else {
            dao = new GenericCompositeDao<ID, String>(keyspace, idMeta.getValueSerializer(), STRING_SRZ,
                    externalTableName);
        }

        propertyMeta.setExternalWideMapProperties(new ExternalWideMapProperties<ID>(externalTableName, dao, idMeta
                .getValueSerializer()));

        propertyMetasTL.get().put(propertyMeta.getPropertyName(), propertyMeta);
    }
}
