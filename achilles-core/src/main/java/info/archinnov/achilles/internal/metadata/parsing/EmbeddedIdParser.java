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

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.reflections.ReflectionUtils.getAllConstructors;
import static org.reflections.ReflectionUtils.getAllFields;
import static org.reflections.ReflectionUtils.withParametersCount;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.TimeUUID;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdProperties;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdPropertiesBuilder;
import info.archinnov.achilles.internal.metadata.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.internal.validation.Validator;

public class EmbeddedIdParser {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedIdParser.class);
    protected EntityIntrospector entityIntrospector = new EntityIntrospector();

    private PropertyFilter filter = new PropertyFilter();

    public EmbeddedIdProperties parseEmbeddedId(Class<?> embeddedIdClass) {
        log.debug("Parse embedded id class {} ", embeddedIdClass.getCanonicalName());

        checkForDefaultConstructor(embeddedIdClass);

        Map<Integer, Field> components = extractComponentsOrdering(embeddedIdClass);
        validateConsistentPartitionKeys(components, embeddedIdClass.getCanonicalName());
        final List<ClusteringOrder> clusteringOrders = extractClusteredOrder(embeddedIdClass);
        EmbeddedIdProperties embeddedIdProperties = buildComponentMetas(embeddedIdClass, components, clusteringOrders);

        log.trace("Built embeddedId properties : {}", embeddedIdProperties);
        return embeddedIdProperties;
    }

    private Map<Integer, Field> extractComponentsOrdering(Class<?> embeddedIdClass) {
        log.trace("Extract components ordering from embedded id class {} ", embeddedIdClass.getCanonicalName());

        String embeddedIdClassName = embeddedIdClass.getCanonicalName();
        Map<Integer, Field> components = new TreeMap<>();

        @SuppressWarnings("unchecked")
        Set<Field> candidateFields = getAllFields(embeddedIdClass, ReflectionUtils.<Field>withAnnotation(Order.class));

        Set<Integer> orders = new HashSet<>();
        int orderSum = 0;
        int componentCount = candidateFields.size();

        for (Field candidateField : candidateFields) {
            Order orderAnnotation = candidateField.getAnnotation(Order.class);
            int order = orderAnnotation.value();
            Class<?> componentType = candidateField.getType();
            orderSum = validateNoDuplicateOrderAndType(embeddedIdClassName, orders, orderSum, order, componentType);
            components.put(order, candidateField);
        }

        validateConsistentOrdering(embeddedIdClassName, orderSum, componentCount);
        Validator.validateBeanMappingTrue(componentCount > 1,
                "There should be at least 2 fields annotated with @Order for the @EmbeddedId class '%s'",
                embeddedIdClass.getCanonicalName());
        return components;

    }

    private List<ClusteringOrder> extractClusteredOrder(Class<?> embeddedIdClass) {
        log.debug("Extract clustering component order from embedded id class {} ",embeddedIdClass.getCanonicalName());

        List<ClusteringOrder> sortOrders = new LinkedList<>();

        @SuppressWarnings("unchecked")
        Set<Field> candidateFields = getAllFields(embeddedIdClass, ReflectionUtils.withAnnotation(Order.class));
        final List<Field> clusteringFields = FluentIterable.from(candidateFields).filter(new Predicate<Field>() {
            @Override
            public boolean apply(Field field) {
                Order orderAnnotation = field.getAnnotation(Order.class);
                return !filter.hasAnnotation(field, PartitionKey.class) && orderAnnotation.value() > 1;
            }
        }).toSortedList(new Comparator<Field>() {
            @Override
            public int compare(Field o1, Field o2) {
                Order order1 = o1.getAnnotation(Order.class);
                Order order2 = o2.getAnnotation(Order.class);
                return new Integer(order1.value()).compareTo(new Integer(order2.value()));
            }
        });

        for (Field clusteringField : clusteringFields) {
            final Order order = clusteringField.getAnnotation(Order.class);
            final String columnName = extractColumnName(clusteringField);
            sortOrders.add(new ClusteringOrder(columnName, order.reversed() ? Sorting.DESC : Sorting.ASC));
        }

        return sortOrders;

    }

    private int validateNoDuplicateOrderAndType(String embeddedIdClassName, Set<Integer> orders, int orderSum,
            int order, Class<?> componentType) {
        log.debug("Validate type and component ordering for embedded id class {} ", embeddedIdClassName);
        Validator.validateBeanMappingTrue(orders.add(order), "The order '%s' is duplicated in @EmbeddedId class '%s'",
                order, embeddedIdClassName);

        orderSum += order;

        PropertyParsingValidator.validateAllowedTypes(componentType, PropertyParser.allowedTypes, "The class '"
                + componentType.getCanonicalName() + "' is not a valid component type for the @EmbeddedId class '"
                + embeddedIdClassName + "'");
        return orderSum;
    }

    private void validateConsistentOrdering(String embeddedIdClassName, int orderSum, int componentCount) {
        int check = (componentCount * (componentCount + 1)) / 2;

        log.debug("Validate component ordering for @EmbeddedId class {} ", embeddedIdClassName);

        Validator.validateBeanMappingTrue(orderSum == check,
                "The component ordering is wrong for @EmbeddedId class '%s'", embeddedIdClassName);
    }

    private void validateConsistentPartitionKeys(Map<Integer, Field> componentsOrdering, String embeddedIdClassName) {
        log.debug("Validate composite partition key component ordering for @EmbeddedId class {} ", embeddedIdClassName);
        int orderSum = 0;
        int orderCount = 0;
        for (Integer order : componentsOrdering.keySet()) {
            Field componentField = componentsOrdering.get(order);
            if (filter.hasAnnotation(componentField, PartitionKey.class)) {
                orderSum = orderSum + order;
                orderCount++;
            }
        }

        int check = (orderCount * (orderCount + 1)) / 2;
        Validator.validateBeanMappingTrue(orderSum == check, "The composite partition key ordering is wrong for @EmbeddedId class '%s'", embeddedIdClassName);
    }

    private EmbeddedIdProperties buildComponentMetas(Class<?> embeddedIdClass, Map<Integer, Field> components,
            List<ClusteringOrder> clusteringOrders) {

        log.debug("Build components meta data for embedded id class {}", embeddedIdClass.getCanonicalName());
        EmbeddedIdPropertiesBuilder partitionKeysBuilder = new EmbeddedIdPropertiesBuilder();
        EmbeddedIdPropertiesBuilder clusteringKeysBuilder = new EmbeddedIdPropertiesBuilder();
        clusteringKeysBuilder.setClusteringOrders(clusteringOrders);
        EmbeddedIdPropertiesBuilder embeddedIdPropertiesBuilder = new EmbeddedIdPropertiesBuilder();

        boolean hasPartitionKeyAnnotation = buildPartitionAndClusteringKeys(embeddedIdClass, components,
                partitionKeysBuilder, clusteringKeysBuilder, embeddedIdPropertiesBuilder);

        if (!hasPartitionKeyAnnotation) {
            partitionKeysBuilder.addComponentName(clusteringKeysBuilder.removeFirstComponentName());
            partitionKeysBuilder.addComponentClass(clusteringKeysBuilder.removeFirstComponentClass());
            partitionKeysBuilder.addComponentField(clusteringKeysBuilder.removeFirstComponentField());
            partitionKeysBuilder.addComponentGetter(clusteringKeysBuilder.removeFirstComponentGetter());
            partitionKeysBuilder.addComponentSetter(clusteringKeysBuilder.removeFirstComponentSetter());
        }

        return embeddedIdPropertiesBuilder.buildEmbeddedIdProperties(partitionKeysBuilder.buildPartitionKeys(),
                clusteringKeysBuilder.buildClusteringKeys());
    }

    private boolean buildPartitionAndClusteringKeys(Class<?> embeddedIdClass, Map<Integer, Field> components,
            EmbeddedIdPropertiesBuilder partitionKeysBuilder,EmbeddedIdPropertiesBuilder clusteringKeysBuilder,
            EmbeddedIdPropertiesBuilder embeddedIdPropertiesBuilder) {
        log.debug("Build Components meta data for embedded id class {}", embeddedIdClass.getCanonicalName());

        boolean hasPartitionKeyAnnotation = false;
        for (Integer order : components.keySet()) {
            Field compositeKeyField = components.get(order);
            Class<?> componentClass = compositeKeyField.getType();
            String componentName;
            Method componentGetter = entityIntrospector.findGetter(embeddedIdClass, compositeKeyField);
            Method componentSetter = entityIntrospector.findSetter(embeddedIdClass, compositeKeyField);

            componentName = extractColumnName(compositeKeyField);

            embeddedIdPropertiesBuilder.addComponentName(componentName);
            embeddedIdPropertiesBuilder.addComponentClass(componentClass);
            embeddedIdPropertiesBuilder.addComponentField(compositeKeyField);
            embeddedIdPropertiesBuilder.addComponentGetter(componentGetter);
            embeddedIdPropertiesBuilder.addComponentSetter(componentSetter);

            if (filter.hasAnnotation(compositeKeyField, TimeUUID.class)) {
                embeddedIdPropertiesBuilder.addTimeUUIDComponent(componentName);
            }

            if (filter.hasAnnotation(compositeKeyField, PartitionKey.class)) {
                partitionKeysBuilder.addComponentName(componentName);
                partitionKeysBuilder.addComponentClass(componentClass);
                partitionKeysBuilder.addComponentField(compositeKeyField);
                partitionKeysBuilder.addComponentGetter(componentGetter);
                partitionKeysBuilder.addComponentSetter(componentSetter);
                hasPartitionKeyAnnotation = true;
            } else {
                clusteringKeysBuilder.addComponentName(componentName);
                clusteringKeysBuilder.addComponentClass(componentClass);
                clusteringKeysBuilder.addComponentField(compositeKeyField);
                clusteringKeysBuilder.addComponentGetter(componentGetter);
                clusteringKeysBuilder.addComponentSetter(componentSetter);
            }

        }
        return hasPartitionKeyAnnotation;
    }

    private String extractColumnName(Field compositeKeyField) {
        String componentName;
        Column column = compositeKeyField.getAnnotation(Column.class);

        if (column != null && isNotBlank(column.name())) {
            componentName = column.name();
        }
        else {
            componentName = compositeKeyField.getName();
        }

        if (column != null && column.staticColumn()) {
            throw new AchillesBeanMappingException(String.format("The property '%s' of class '%s' cannot be a static column because it belongs to the primary key",componentName,compositeKeyField.getDeclaringClass().getCanonicalName()));
        }
        return componentName;
    }

    private void checkForDefaultConstructor(Class<?> embeddedIdClass) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Set<Constructor> defaultConstructors = getAllConstructors(embeddedIdClass, withParametersCount(0));

        Validator.validateBeanMappingFalse(defaultConstructors.isEmpty(),
                "The @EmbeddedId class '%s' should have a public default constructor",
                embeddedIdClass.getCanonicalName());

    }
}
