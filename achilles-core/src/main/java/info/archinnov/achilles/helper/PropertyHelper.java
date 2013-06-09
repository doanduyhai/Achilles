package info.archinnov.achilles.helper;

import static org.apache.commons.lang.StringUtils.*;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.Key;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
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
import javax.persistence.Column;
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
    protected EntityIntrospector entityIntrospector = new EntityIntrospector();
    private MethodInvoker invoker = new MethodInvoker();

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
        List<String> componentNames = new ArrayList<String>();
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
                    throw new AchillesBeanMappingException("The order '" + order
                            + "' is duplicated in MultiKey class '" + keyClass.getCanonicalName() + "'");
                } else {
                    orders.add(order);
                }
                keySum += order;

                Class<?> keySubType = multiKeyField.getType();
                PropertyParsingValidator.validateAllowedTypes(keySubType, allowedTypes,
                        "The class '" + keySubType.getCanonicalName()
                                + "' is not a valid key type for the MultiKey class '" + keyClass.getCanonicalName()
                                + "'");

                components.put(keyAnnotation.order(), multiKeyField);

            }
        }

        int check = (keyCount * (keyCount + 1)) / 2;

        log.debug("Validate key ordering multikey class {} ", keyClass.getCanonicalName());

        Validator.validateBeanMappingTrue(keySum == check,
                "The key orders is wrong for MultiKey class '" + keyClass.getCanonicalName() + "'");

        List<Integer> orderList = new ArrayList<Integer>(components.keySet());
        Collections.sort(orderList);
        for (Integer order : orderList) {
            Field multiKeyField = components.get(order);
            Column column = multiKeyField.getAnnotation(Column.class);
            if (column != null) {
                if (isNotBlank(column.name())) {
                    componentNames.add(column.name());
                } else {
                    componentNames.add(multiKeyField.getName());
                }
            }
            componentGetters.add(entityIntrospector.findGetter(keyClass, multiKeyField));
            componentSetters.add(entityIntrospector.findSetter(keyClass, multiKeyField));
            componentClasses.add(multiKeyField.getType());
        }

        Validator.validateBeanMappingNotEmpty(componentClasses, "No field with @Key annotation found in the class '"
                + keyClass.getCanonicalName() + "'");
        Validator.validateInstantiable(keyClass);
        if (componentNames.size() > 0) {
            Validator.validateBeanMappingTrue(
                    componentClasses.size() == componentNames.size(),
                    "There should be the same number of @Key than @Column annotation in the class '"
                            + keyClass.getCanonicalName() + "'");
        }

        MultiKeyProperties multiKeyProperties = new MultiKeyProperties();
        multiKeyProperties.setComponentClasses(componentClasses);
        multiKeyProperties.setComponentNames(componentNames);
        multiKeyProperties.setComponentGetters(componentGetters);
        multiKeyProperties.setComponentSetters(componentSetters);

        log.trace("Built multi key properties : {}", multiKeyProperties);
        return multiKeyProperties;
    }

    public int findLastNonNullIndexForComponents(String propertyName, List<Object> keyValues) {
        boolean nullFlag = false;
        int lastNotNullIndex = 0;
        for (Object keyValue : keyValues) {
            if (keyValue != null) {
                if (nullFlag) {
                    throw new IllegalArgumentException(
                            "There should not be any null value between two non-null keys of WideMap '"
                                    + propertyName + "'");
                }
                lastNotNullIndex++;
            } else {
                nullFlag = true;
            }
        }
        lastNotNullIndex--;

        log.trace("Last non null index for components of property {} : {}", propertyName, lastNotNullIndex);
        return lastNotNullIndex;
    }

    public <K> void checkBounds(PropertyMeta<?, ?> propertyMeta, K start, K end, WideMap.OrderingMode ordering,
            boolean clusteringId) {
        log.trace("Check composites {} / {} with respect to ordering mode {}", start, end, ordering.name());
        if (start != null && end != null) {
            if (propertyMeta.isSingleKey()) {
                Comparable<K> startComp = (Comparable<K>) start;

                if (WideMap.OrderingMode.DESCENDING.equals(ordering)) {
                    Validator.validateTrue(startComp.compareTo(end) >= 0,
                            "For reverse range query, start value should be greater or equal to end value");
                } else {
                    Validator.validateTrue(startComp.compareTo(end) <= 0,
                            "For range query, start value should be lesser or equal to end value");
                }
            } else {
                List<Method> componentGetters = propertyMeta.getMultiKeyProperties().getComponentGetters();
                String propertyName = propertyMeta.getPropertyName();

                List<Object> startComponentValues = invoker.determineMultiKeyValues(start, componentGetters);
                List<Object> endComponentValues = invoker.determineMultiKeyValues(end, componentGetters);

                if (clusteringId) {
                    Validator.validateNotNull(startComponentValues.get(0),
                            "Partition key should not be null for start clustering key : " + startComponentValues);
                    Validator.validateNotNull(endComponentValues.get(0),
                            "Partition key should not be null for end clustering key : " + endComponentValues);
                    Validator.validateTrue(startComponentValues.get(0).equals(endComponentValues.get(0)),
                            "Partition key should be equals for start and end clustering keys : ["
                                    + startComponentValues + "," + endComponentValues + "]");
                }

                findLastNonNullIndexForComponents(propertyName, startComponentValues);
                findLastNonNullIndexForComponents(propertyName, endComponentValues);

                for (int i = 0; i < startComponentValues.size(); i++) {

                    Comparable<Object> startValue = (Comparable<Object>) startComponentValues.get(i);
                    Object endValue = endComponentValues.get(i);

                    if (WideMap.OrderingMode.DESCENDING.equals(ordering)) {
                        if (startValue != null && endValue != null) {
                            Validator
                                    .validateTrue(startValue.compareTo(endValue) >= 0,
                                            "For multiKey descending range query, startKey value should be greater or equal to end endKey");
                        }

                    } else {
                        if (startValue != null && endValue != null) {
                            Validator
                                    .validateTrue(startValue.compareTo(endValue) <= 0,
                                            "For multiKey ascending range query, startKey value should be lesser or equal to end endKey");
                        }
                    }
                }
            }
        }
    }

    public <T> Class<T> inferValueClassForListOrSet(Type genericType, Class<?> entityClass) {
        log.debug("Infer parameterized value class for collection type {} of entity class {} ",
                genericType.toString(), entityClass.getCanonicalName());

        Class<T> valueClass;
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            if (actualTypeArguments.length > 0) {
                valueClass = (Class<T>) actualTypeArguments[actualTypeArguments.length - 1];
            } else {
                throw new AchillesBeanMappingException("The type '" + genericType.getClass().getCanonicalName()
                        + "' of the entity '" + entityClass.getCanonicalName() + "' should be parameterized");
            }
        } else {
            throw new AchillesBeanMappingException("The type '" + genericType.getClass().getCanonicalName()
                    + "' of the entity '" + entityClass.getCanonicalName() + "' should be parameterized");
        }

        log.trace("Inferred value class : {}", valueClass.getCanonicalName());

        return valueClass;
    }

    public boolean isLazy(Field field) {
        log.debug("Check @Lazy annotation on field {} of class {}", field.getName(), field
                .getDeclaringClass()
                .getCanonicalName());

        boolean lazy = false;
        if (field.getAnnotation(Lazy.class) != null) {
            lazy = true;
        }
        return lazy;
    }

    public boolean hasConsistencyAnnotation(Field field) {
        log.debug("Check @Consistency annotation on field {} of class {}", field.getName(), field
                .getDeclaringClass()
                .getCanonicalName());

        boolean consistency = false;
        if (field.getAnnotation(Consistency.class) != null) {
            consistency = true;
        }
        return consistency;
    }

    public static <T> boolean isSupportedType(Class<T> valueClass) {
        return allowedTypes.contains(valueClass);
    }

    public <T> Pair<ConsistencyLevel, ConsistencyLevel> findConsistencyLevels(Field field,
            AchillesConsistencyLevelPolicy policy) {
        log.debug("Find consistency configuration for field {} of class {}", field.getName(), field
                .getDeclaringClass()
                .getCanonicalName());

        Consistency clevel = field.getAnnotation(Consistency.class);

        ConsistencyLevel defaultGlobalRead = entityIntrospector.getDefaultGlobalReadConsistency(policy);
        ConsistencyLevel defaultGlobalWrite = entityIntrospector.getDefaultGlobalWriteConsistency(policy);

        if (clevel != null) {
            defaultGlobalRead = clevel.read();
            defaultGlobalWrite = clevel.write();
        }

        log.trace("Found consistency levels : {} / {}", defaultGlobalRead, defaultGlobalWrite);
        return new Pair<ConsistencyLevel, ConsistencyLevel>(defaultGlobalRead, defaultGlobalWrite);
    }
}
