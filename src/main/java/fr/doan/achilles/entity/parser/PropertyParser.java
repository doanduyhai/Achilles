package fr.doan.achilles.entity.parser;

import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SET;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.entity.metadata.factory.PropertyMetaFactory.factory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.PropertyHelper;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.type.MultiKey;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.exception.BeanMappingException;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.validation.Validator;

public class PropertyParser {

    private PropertyFilter filter = new PropertyFilter();

    public PropertyParser() {
    }

    private PropertyHelper propertyHelper = new PropertyHelper();
    private EntityHelper entityHelper = new EntityHelper();

    @SuppressWarnings("unchecked")
    public <K, V> PropertyMeta<K, V> parse(Class<?> beanClass, Field field, String propertyName) {
        Class<?> fieldType = field.getType();

        PropertyMeta<K, V> propertyMeta;

        if (List.class.isAssignableFrom(fieldType)) {
            propertyMeta = (PropertyMeta<K, V>) parseListProperty(beanClass, field, propertyName, fieldType);
        }

        else if (Set.class.isAssignableFrom(fieldType)) {
            propertyMeta = (PropertyMeta<K, V>) parseSetProperty(beanClass, field, propertyName, fieldType);
        }

        else if (Map.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseMapProperty(beanClass, field, propertyName, fieldType);
        }

        else if (WideMap.class.isAssignableFrom(fieldType)) {
            propertyMeta = parseWideMapProperty(beanClass, field, propertyName, fieldType);
        }

        else {
            propertyMeta = (PropertyMeta<K, V>) parseSimpleProperty(beanClass, field, propertyName);
        }
        return propertyMeta;
    }

    @SuppressWarnings("unchecked")
    private <V> PropertyMeta<Void, V> parseSimpleProperty(Class<?> beanClass, Field field, String propertyName) {
        Validator.validateSerializable(field.getType(), "property '" + field.getName() + "'");
        Method[] accessors = entityHelper.findAccessors(beanClass, field);

        PropertyMeta<Void, V> propertyMeta = null;
        if (filter.hasAnnotation(field, JoinColumn.class)) {
            PropertyType type = PropertyType.JOIN_SIMPLE;

            JoinProperties joinProperties = new JoinProperties();

            if (filter.hasAnnotation(field, OneToOne.class) || filter.hasAnnotation(field, OneToMany.class)
                    || filter.hasAnnotation(field, ManyToMany.class)) {
                throw new BeanMappingException(
                        "Incorrect annotation. Only @ManyToOne is allowed for the join property '" + field.getName()
                                + "'");
            }

            else if (filter.hasAnnotation(field, ManyToOne.class)) {
                ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
                joinProperties.addCascadeType(Arrays.asList(manyToOne.cascade()));
            } else {
                throw new BeanMappingException("Missing @ManyToOne annotation for the join property '"
                        + field.getName() + "'");
            }

            propertyMeta = factory((Class<V>) field.getType()).type(type) //
                    .propertyName(propertyName).accessors(accessors) //
                    .joinProperties(joinProperties).build();
        } else {

            PropertyType type = propertyHelper.isLazy(field) ? LAZY_SIMPLE : SIMPLE;
            propertyMeta = factory((Class<V>) field.getType()).type(type).propertyName(propertyName)
                    .accessors(accessors).build();
        }

        return propertyMeta;
    }

    @SuppressWarnings("unchecked")
    private <V> PropertyMeta<Void, V> parseListProperty(Class<?> beanClass, Field field, String propertyName,
            Class<?> fieldType) {

        Class<?> valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClass(genericType);

        Validator.validateSerializable(valueClass, "list value type of '" + field.getName() + "'");
        Method[] accessors = entityHelper.findAccessors(beanClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_LIST : LIST;

        return factory((Class<V>) valueClass).type(type).propertyName(propertyName).accessors(accessors).build();

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <V> PropertyMeta<Void, V> parseSetProperty(Class<?> beanClass, Field field, String propertyName,
            Class<?> fieldType) {

        Class valueClass;
        Type genericType = field.getGenericType();

        valueClass = propertyHelper.inferValueClass(genericType);
        Validator.validateSerializable(valueClass, "set value type of '" + field.getName() + "'");
        Method[] accessors = entityHelper.findAccessors(beanClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_SET : SET;

        return factory((Class<V>) valueClass).type(type).propertyName(propertyName).accessors(accessors).build();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <K, V> PropertyMeta<K, V> parseMapProperty(Class<?> beanClass, Field field, String propertyName,
            Class<?> fieldType) {

        Class valueClass;
        Class keyType;

        Type genericType = field.getGenericType();

        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 1) {
                keyType = (Class) actualTypeArguments[0];
                valueClass = (Class) actualTypeArguments[1];
            } else {
                keyType = Object.class;
                valueClass = Object.class;
            }
        } else {
            keyType = Object.class;
            valueClass = Object.class;
        }
        Validator.validateSerializable(valueClass, "map value type of '" + field.getName() + "'");
        Validator.validateSerializable(keyType, "map key type of '" + field.getName() + "'");
        Method[] accessors = entityHelper.findAccessors(beanClass, field);
        PropertyType type = propertyHelper.isLazy(field) ? LAZY_MAP : MAP;

        return factory(keyType, valueClass).type(type).propertyName(propertyName).accessors(accessors).build();

    }

    @SuppressWarnings("unchecked")
    private <K, V> PropertyMeta<K, V> parseWideMapProperty(Class<?> beanClass, Field field, String propertyName,
            Class<?> fieldType) {
        List<Class<?>> componentClasses = new ArrayList<Class<?>>();
        List<Method> componentGetters = new ArrayList<Method>();
        List<Method> componentSetters = new ArrayList<Method>();

        Class<K> keyClass;
        Class<V> valueClass;

        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 1) {
                keyClass = (Class<K>) actualTypeArguments[0];
                valueClass = (Class<V>) actualTypeArguments[1];

                if (MultiKey.class.isAssignableFrom(keyClass)) {
                    propertyHelper.parseMultiKey(componentClasses, componentGetters, componentSetters, keyClass);
                } else {
                    Validator
                            .validateAllowedTypes(
                                    keyClass,
                                    propertyHelper.allowedTypes,
                                    "The class '"
                                            + keyClass.getCanonicalName()
                                            + "' is not allowed as WideMap key. Did you forget to implement MultiKey interface ?");
                }
            } else {
                throw new IncorrectTypeException(
                        "The WideMap type should be parameterized with <K,V> for the entity "
                                + beanClass.getCanonicalName());
            }
        } else {
            throw new IncorrectTypeException("The WideMap type should be parameterized for the entity "
                    + beanClass.getCanonicalName());
        }

        Validator.validateSerializable(valueClass, "value type of " + field.getName());
        Method[] accessors = entityHelper.findAccessors(beanClass, field);

        JoinProperties joinProperties = null;

        PropertyType type = PropertyType.WIDE_MAP;
        if (filter.hasAnnotation(field, JoinColumn.class)) {
            type = PropertyType.JOIN_WIDE_MAP;

            joinProperties = new JoinProperties();

            if (filter.hasAnnotation(field, OneToOne.class) || filter.hasAnnotation(field, OneToMany.class)
                    || filter.hasAnnotation(field, ManyToOne.class)) {
                throw new BeanMappingException(
                        "Incorrect annotation. Only @ManyToMany is allowed for the join property '" + field.getName()
                                + "'");
            }

            else if (filter.hasAnnotation(field, ManyToMany.class)) {
                ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
                joinProperties.addCascadeType(Arrays.asList(manyToMany.cascade()));
            } else {
                throw new BeanMappingException("Missing @ManyToMany annotation for the join property '"
                        + field.getName() + "'");
            }

        }

        if (componentClasses.size() == 0) {
            return factory(keyClass, valueClass).type(type).propertyName(propertyName).accessors(accessors)
                    .singleKey(true).joinProperties(joinProperties).build();

        } else {
            return factory(keyClass, valueClass).type(type).propertyName(propertyName).accessors(accessors)
                    .singleKey(false) //
                    .componentClasses(componentClasses) //
                    .componentGetters(componentGetters) //
                    .componentSetters(componentSetters) //
                    .joinProperties(joinProperties) //
                    .build();
        }
    }
}
