package info.archinnov.achilles.entity.parsing;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.reflections.ReflectionUtils.*;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.helper.PropertyHelper;
import info.archinnov.achilles.validation.Validator;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Column;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;

public class CompoundKeyParser
{

    private static final Logger log = LoggerFactory.getLogger(PropertyHelper.class);
    protected EntityIntrospector entityIntrospector = new EntityIntrospector();

    private static final Predicate<Annotation> hasOrderAnnotation = new Predicate<Annotation>()
    {

        @Override
        public boolean apply(Annotation annot)
        {
            return annot.annotationType().equals(Order.class);
        }
    };

    private static final Predicate<Annotation> hasCorrectJsonPropertyAnnotation = new Predicate<Annotation>()
    {

        @Override
        public boolean apply(Annotation annot)
        {
            boolean accept = annot.annotationType().equals(JsonProperty.class);

            if (accept)
                Validator
                        .validateBeanMappingTrue(isNotBlank(((JsonProperty) annot).value()),
                                "@JsonProperty on constructor param should have a 'value' attribute for deserialization");
            return accept;
        }
    };

    private static final Function<Annotation, Integer> retrieveOrder = new Function<Annotation, Integer>()
    {

        @Override
        public Integer apply(Annotation annot)
        {
            return ((Order) annot).value();
        }
    };

    private static final Function<Annotation, String> retrieveJsonPropertyName = new Function<Annotation, String>()
    {

        @Override
        public String apply(Annotation annot)
        {
            return ((JsonProperty) annot).value();
        }
    };

    public EmbeddedIdProperties parseCompoundKey(Class<?> keyClass)
    {
        log.debug("Parse multikey class {} ", keyClass.getCanonicalName());

        List<Class<?>> componentClasses = new ArrayList<Class<?>>();
        List<String> componentNames = new ArrayList<String>();
        List<Method> componentGetters = new ArrayList<Method>();
        List<Method> componentSetters = new ArrayList<Method>();
        Map<Integer, Field> components = new HashMap<Integer, Field>();

        Constructor<?> constructor = scanAnnotatedFields(keyClass, components);
        if (components.isEmpty())
        {
            constructor = scannConstructorParams(keyClass, components);
        }

        Validator.validateBeanMappingTrue(
                components.size() > 1,
                "There should be at least 2 components for the @CompoundKey class '%s'", keyClass.getCanonicalName());

        EmbeddedIdProperties embeddedIdProperties = buildComponentMetas(keyClass,
                componentClasses, componentNames,
                componentGetters, componentSetters,
                components, constructor);

        log.trace("Built compound key properties : {}", embeddedIdProperties);
        return embeddedIdProperties;
    }

    private Constructor<?> scanAnnotatedFields(Class<?> keyClass, Map<Integer, Field> components)
    {

        @SuppressWarnings("unchecked")
        Set<Field> candidateFields = getFields(keyClass,
                ReflectionUtils.<Field> withAnnotation(Order.class));

        Set<Integer> orders = new HashSet<Integer>();
        int orderSum = 0;
        int componentCount = candidateFields.size();

        for (Field candidateField : candidateFields)
        {
            int order = candidateField.getAnnotation(Order.class).value();
            orderSum = validateNoDuplicateOrderAndType(keyClass, orders, orderSum, order,
                    candidateField.getType());
            components.put(order, candidateField);
        }

        validateConsistentOrdering(keyClass, orderSum, componentCount);

        if (components.isEmpty())
        {
            return null;
        }
        else
        {
            @SuppressWarnings(
            {
                    "unchecked",
                    "rawtypes"
            })
            Set<Constructor> defaultConstructors = getAllConstructors(keyClass,
                    withParametersCount(0));

            Validator.validateBeanMappingFalse(defaultConstructors.isEmpty(),
                    "The @CompoundKey class '%s' should have a public default constructor",
                    keyClass.getCanonicalName());

            return defaultConstructors.iterator().next();
        }
    }

    private Constructor<?> scannConstructorParams(Class<?> keyClass, Map<Integer, Field> components)
    {
        Set<Integer> orders = new HashSet<Integer>();
        int orderSum = 0;
        int componentCount = 0;

        Constructor<?> constructor = findMatchingConstructor(keyClass);
        Annotation[][] allAnnotations = constructor.getParameterAnnotations();
        Class<?>[] paramTypes = constructor.getParameterTypes();
        Set<String> uniqueJsonPropertyNames = new HashSet<String>();

        for (int i = 0; i < paramTypes.length; i++)
        {
            Annotation[] paramAnnotations = allAnnotations[i];

            String propertyName = validateJsonPropertyAnnotation(keyClass, constructor,
                    paramAnnotations,
                    uniqueJsonPropertyNames);
            int order = retrieveOrder(keyClass, constructor, paramAnnotations);
            orderSum = validateNoDuplicateOrderAndType(keyClass, orders, orderSum, order,
                    paramTypes[i]);
            Field matchingField = findFieldMatchingConstructorParam(keyClass, paramTypes[i],
                    propertyName);
            components.put(order, matchingField);
            componentCount++;
        }

        validateConsistentOrdering(keyClass, orderSum, componentCount);

        return constructor;
    }

    private Constructor<?> findMatchingConstructor(Class<?> keyClass)
    {
        @SuppressWarnings(
        {
                "unchecked",
                "rawtypes"
        })
        Set<Constructor> candidateConstructors = ReflectionUtils.getConstructors(keyClass,
                Predicates.and((Predicate) withAnyParameterAnnotation(Order.class),
                        (Predicate) withAnyParameterAnnotation(JsonProperty.class),
                        (Predicate) withAnnotation(JsonCreator.class)));

        Validator
                .validateBeanMappingTrue(
                        candidateConstructors.size() == 1,
                        "There should be exactly one constructor for @CompoundKey class '"
                                + keyClass.getCanonicalName()
                                + "' annotated by @JsonCreator and all arguments annotated by @Order AND @JsonProperty");

        Constructor<?> constructor = candidateConstructors.iterator().next();
        return constructor;
    }

    private String validateJsonPropertyAnnotation(Class<?> keyClass, Constructor<?> constructor,
            Annotation[] paramAnnotations, Set<String> uniqueJsonPropertyNames)
    {

        Optional<String> jsonPropertyName = FluentIterable
                .from(Arrays.asList(paramAnnotations))
                .filter(hasCorrectJsonPropertyAnnotation)
                .transform(retrieveJsonPropertyName)
                .first();

        Validator
                .validateBeanMappingTrue(
                        jsonPropertyName.isPresent(),
                        "The constructor '%s' of @CompoundKey class '%s' should have all its params annotated with @Order AND @JsonProperty",
                        constructor.toString(), keyClass.getCanonicalName());

        Validator.validateBeanMappingTrue(uniqueJsonPropertyNames.add(jsonPropertyName.get()),
                "The property names defined by @JsonProperty should be unique for the @CompoundKey class '%s'",
                keyClass.getCanonicalName());

        return jsonPropertyName.get();

    }

    private int retrieveOrder(Class<?> keyClass, Constructor<?> constructor,
            Annotation[] paramAnnotations)
    {
        Optional<Integer> orderO = FluentIterable
                .from(Arrays.asList(paramAnnotations))
                .filter(hasOrderAnnotation)
                .transform(retrieveOrder)
                .first();

        Validator
                .validateBeanMappingTrue(
                        orderO.isPresent(),
                        "The constructor '%s' of @CompoundKey class '%s' should have all its params annotated with @Order AND @JsonProperty",
                        constructor.toString(), keyClass.getCanonicalName());

        int order = orderO.get();
        return order;
    }

    private Field findFieldMatchingConstructorParam(Class<?> keyClass, Class<?> componentType,
            String propertyName)
    {
        @SuppressWarnings("unchecked")
        Set<Field> matchingFields = getFields(keyClass, Predicates.and(
                withType(componentType),
                withName(propertyName)));

        Validator.validateBeanMappingTrue(
                matchingFields.size() == 1,
                "Cannot find field of type '%s' and name '%s' in the @CompoundKey class %s"
                , componentType.getCanonicalName(), propertyName, keyClass.getCanonicalName());

        Field matchingField = matchingFields.iterator().next();
        return matchingField;
    }

    // Shared methods
    private int validateNoDuplicateOrderAndType(Class<?> keyClass, Set<Integer> orders,
            int orderSum, int order,
            Class<?> componentType)
    {
        Validator.validateBeanMappingTrue(orders.add(order),
                "The order '%s' is duplicated in @CompoundKey class '%s'", order, keyClass.getCanonicalName());

        orderSum += order;

        PropertyParsingValidator.validateAllowedTypes(
                componentType,
                PropertyHelper.allowedTypes,
                "The class '" + componentType.getCanonicalName()
                        + "' is not a valid component type for the @CompoundKey class '"
                        + keyClass.getCanonicalName()
                        + "'");
        return orderSum;
    }

    private void validateConsistentOrdering(Class<?> keyClass, int orderSum, int keyCount)
    {
        int check = (keyCount * (keyCount + 1)) / 2;

        log.debug("Validate key ordering compound key class {} ", keyClass.getCanonicalName());

        Validator.validateBeanMappingTrue(orderSum == check,
                "The key orders is wrong for @CompoundKey class '%s'", keyClass.getCanonicalName());
    }

    private EmbeddedIdProperties buildComponentMetas(Class<?> keyClass,
            List<Class<?>> componentClasses,
            List<String> componentNames,
            List<Method> componentGetters, List<Method> componentSetters,
            Map<Integer, Field> components,
            Constructor<?> constructor)
    {

        List<Integer> orderList = new ArrayList<Integer>(components.keySet());
        Collections.sort(orderList);

        for (Integer order : orderList)
        {
            Field compoundKeyField = components.get(order);
            Column column = compoundKeyField.getAnnotation(Column.class);

            if (column != null && isNotBlank(column.name()))
                componentNames.add(column.name());
            else
                componentNames.add(compoundKeyField.getName());

            componentGetters.add(entityIntrospector.findGetter(keyClass, compoundKeyField));
            if (constructor.getParameterTypes().length == 0)
                componentSetters.add(entityIntrospector.findSetter(keyClass, compoundKeyField));

            componentClasses.add(compoundKeyField.getType());
        }

        Validator.validateBeanMappingNotEmpty(componentClasses,
                "No field or constructor param with @Order annotation found in the class '%s'",
                keyClass.getCanonicalName());

        EmbeddedIdProperties embeddedIdProperties = new EmbeddedIdProperties();
        embeddedIdProperties.setComponentClasses(componentClasses);
        embeddedIdProperties.setComponentNames(componentNames);
        embeddedIdProperties.setComponentGetters(componentGetters);
        embeddedIdProperties.setComponentSetters(componentSetters);
        embeddedIdProperties.setConstructor(constructor);

        return embeddedIdProperties;
    }
}
