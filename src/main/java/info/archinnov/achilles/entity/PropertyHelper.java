package info.archinnov.achilles.entity;

import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.type.MultiKey;
import info.archinnov.achilles.exception.BeanMappingException;
import info.archinnov.achilles.validation.Validator;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyHelper {
    private static final Logger log = LoggerFactory.getLogger(PropertyHelper.class);

    public static Set<Class<?>> allowedTypes = new HashSet<Class<?>>();
    private EntityHelper entityHelper = new EntityHelper();

    static {
        // Bytes
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

        // Char
        allowedTypes.add(Character.class);

        // Char
        allowedTypes.add(Character.class);

        // Float
        allowedTypes.add(Float.class);
        allowedTypes.add(float.class);

        // Integer
        allowedTypes.add(BigInteger.class);
        allowedTypes.add(Integer.class);
        allowedTypes.add(int.class);

        // Long
        allowedTypes.add(Long.class);
        allowedTypes.add(long.class);

        // Short
        allowedTypes.add(Short.class);
        allowedTypes.add(short.class);

        // String
        allowedTypes.add(String.class);

        // UUID
        allowedTypes.add(UUID.class);

    }

    public PropertyHelper() {
    }

    public MultiKeyProperties parseMultiKey(Class<?> keyClass) {
        log.debug("Parse multikey class {} ", keyClass.getCanonicalName());

        List<Class<?>> componentClasses = new ArrayList<Class<?>>();
        List<Method> componentGetters = new ArrayList<Method>();
        List<Method> componentSetters = new ArrayList<Method>();

        int keySum = 0;
        int keyCount = 0;
        Map<Integer, Field> components = new HashMap<Integer, Field>();

        Set<Integer> orders = new HashSet<Integer>();
        for (Field multiKeyField : keyClass.getDeclaredFields()) {
            Key keyAnnotation = multiKeyField.getAnnotation(Key.class);
            if (keyAnnotation != null) {
                keyCount++;
                int order = keyAnnotation.order();
                if (orders.contains(order)) {
                    throw new BeanMappingException("The order '" + order + "' is duplicated in MultiKey '"
                            + keyClass.getCanonicalName() + "'");
                } else {
                    orders.add(order);
                }
                keySum += order;

                Class<?> keySubType = multiKeyField.getType();
                Validator.validateAllowedTypes(keySubType, allowedTypes,
                        "The class '" + keySubType.getCanonicalName()
                                + "' is not a valid key type for the MultiKey class '" + keyClass.getCanonicalName()
                                + "'");

                components.put(keyAnnotation.order(), multiKeyField);

            }
        }

        int check = (keyCount * (keyCount + 1)) / 2;

        Validator.validateTrue(keySum == check,
                "The key orders is wrong for MultiKey class '" + keyClass.getCanonicalName() + "'");

        List<Integer> orderList = new ArrayList<Integer>(components.keySet());
        Collections.sort(orderList);
        for (Integer order : orderList) {
            Field multiKeyField = components.get(order);
            componentGetters.add(entityHelper.findGetter(keyClass, multiKeyField));
            componentSetters.add(entityHelper.findSetter(keyClass, multiKeyField));
            componentClasses.add(multiKeyField.getType());
        }

        Validator.validateNotEmpty(componentClasses,
                "No field with @Key annotation found in the class '" + keyClass.getCanonicalName() + "'");
        Validator.validateInstantiable(keyClass);

        MultiKeyProperties multiKeyProperties = new MultiKeyProperties();
        multiKeyProperties.setComponentClasses(componentClasses);
        List<Serializer<?>> componentSerializers = new ArrayList<Serializer<?>>();
        for (Class<?> componentClass : componentClasses) {
            componentSerializers.add(SerializerTypeInferer.getSerializer(componentClass));
        }
        multiKeyProperties.setComponentSerializers(componentSerializers);
        multiKeyProperties.setComponentGetters(componentGetters);
        multiKeyProperties.setComponentSetters(componentSetters);

        return multiKeyProperties;
    }

    public Class<?> inferValueClass(Type genericType) {
        Class<?> valueClass;
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 0) {
                valueClass = (Class<?>) actualTypeArguments[actualTypeArguments.length - 1];
            } else {
                valueClass = String.class;
            }
        } else {
            valueClass = Object.class;
        }
        return valueClass;
    }

    public Class<?> inferKeyClass(Type genericType) {
        Class<?> keyClass;
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 1) {
                keyClass = (Class<?>) actualTypeArguments[0];
            } else {
                keyClass = null;
            }
        } else {
            keyClass = null;
        }
        return keyClass;
    }

    public boolean isLazy(Field field) {
        boolean lazy = false;
        if (field.getAnnotation(Lazy.class) != null) {
            lazy = true;
        }
        return lazy;
    }

    public <ID> String determineCompatatorTypeAliasForCompositeCF(PropertyMeta<?, ?> propertyMeta, boolean forCreation) {
        log.debug("Determine the Comparator type alias for composite-base column family using propertyMeta of {} ",
                propertyMeta.getPropertyName());

        Class<?> nameClass = propertyMeta.getKeyClass();
        List<String> comparatorTypes = new ArrayList<String>();
        String comparatorTypesAlias;

        if (MultiKey.class.isAssignableFrom(nameClass)) {

            MultiKeyProperties multiKeyProperties = this.parseMultiKey(nameClass);

            for (Class<?> clazz : multiKeyProperties.getComponentClasses()) {
                Serializer<?> srz = SerializerTypeInferer.getSerializer(clazz);
                if (forCreation) {
                    comparatorTypes.add(srz.getComparatorType().getTypeName());
                } else {
                    comparatorTypes.add("org.apache.cassandra.db.marshal." + srz.getComparatorType().getTypeName());
                }
            }
            if (forCreation) {
                comparatorTypesAlias = "(" + StringUtils.join(comparatorTypes, ',') + ")";
            } else {
                comparatorTypesAlias = "CompositeType(" + StringUtils.join(comparatorTypes, ',') + ")";
            }
        } else {
            String typeAlias = SerializerTypeInferer.getSerializer(nameClass).getComparatorType().getTypeName();
            if (forCreation) {
                comparatorTypesAlias = "(" + typeAlias + ")";
            } else {
                comparatorTypesAlias = "CompositeType(org.apache.cassandra.db.marshal." + typeAlias + ")";
            }
        }

        return comparatorTypesAlias;
    }

    public <K, V> K buildMultiKeyForDynamicComposite(PropertyMeta<K, V> propertyMeta, List<Component<?>> components) {
        K key;

        MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
        Class<K> multiKeyClass = propertyMeta.getKeyClass();
        List<Method> componentSetters = multiKeyProperties.getComponentSetters();
        List<Serializer<?>> serializers = multiKeyProperties.getComponentSerializers();
        try {
            key = multiKeyClass.newInstance();

            for (int i = 2; i < components.size(); i++) {
                Component<?> comp = components.get(i);
                Object compValue = serializers.get(i - 2).fromByteBuffer(comp.getBytes());
                entityHelper.setValueToField(key, componentSetters.get(i - 2), compValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }

        return key;
    }

    public <K, V> K buildMultiKeyForComposite(PropertyMeta<K, V> propertyMeta, List<Component<?>> components) {
        K key;

        MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
        Class<K> multiKeyClass = propertyMeta.getKeyClass();
        List<Method> componentSetters = multiKeyProperties.getComponentSetters();
        List<Serializer<?>> serializers = multiKeyProperties.getComponentSerializers();
        try {
            key = multiKeyClass.newInstance();

            for (int i = 0; i < components.size(); i++) {
                Component<?> comp = components.get(i);
                Object compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
                entityHelper.setValueToField(key, componentSetters.get(i), compValue);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }

        return key;
    }

    public static <T> boolean isSupportedType(Class<T> valueClass) {
        return allowedTypes.contains(valueClass);
    }
}
