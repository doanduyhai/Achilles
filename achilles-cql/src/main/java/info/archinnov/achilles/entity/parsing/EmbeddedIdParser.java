/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.entity.parsing;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.reflections.ReflectionUtils.getAllConstructors;
import static org.reflections.ReflectionUtils.getFields;
import static org.reflections.ReflectionUtils.withParametersCount;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.annotations.TimeUUID;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.metadata.EmbeddedIdPropertiesBuilder;
import info.archinnov.achilles.entity.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.helper.PropertyHelper;
import info.archinnov.achilles.validation.Validator;

public class EmbeddedIdParser {

	private static final Logger log = LoggerFactory.getLogger(PropertyHelper.class);
	protected EntityIntrospector entityIntrospector = new EntityIntrospector();

	private PropertyFilter filter = new PropertyFilter();

	public EmbeddedIdProperties parseEmbeddedId(Class<?> embeddedIdClass) {
		log.debug("Parse embedded id class {} ", embeddedIdClass.getCanonicalName());

		checkForDefaultConstructor(embeddedIdClass);

		Map<Integer, Field> components = extractComponentsOrdering(embeddedIdClass);
		Integer reversedField = extractReversedClusteredKey(embeddedIdClass);
		validateConsistentPartitionKeys(components, embeddedIdClass.getCanonicalName());
		validateReversedClusteredKey(components, reversedField, embeddedIdClass.getCanonicalName());
		EmbeddedIdProperties embeddedIdProperties = buildComponentMetas(embeddedIdClass, components, reversedField
        );

		log.trace("Built embeddedId properties : {}", embeddedIdProperties);
		return embeddedIdProperties;
	}

	private Map<Integer, Field> extractComponentsOrdering(Class<?> embeddedIdClass) {
        log.trace("Extract components ordering from embedded id class {} ", embeddedIdClass.getCanonicalName());

		String embeddedIdClassName = embeddedIdClass.getCanonicalName();
		Map<Integer, Field> components = new TreeMap<Integer, Field>();

		@SuppressWarnings("unchecked")
		Set<Field> candidateFields = getFields(embeddedIdClass, ReflectionUtils.<Field> withAnnotation(Order.class));

		Set<Integer> orders = new HashSet<Integer>();
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

	private Integer extractReversedClusteredKey(Class<?> embeddedIdClass) {
        log.debug("Extract reversed clustering component from embedded id class {} ", embeddedIdClass.getCanonicalName());

        @SuppressWarnings("unchecked")
		Set<Field> candidateFields = getFields(embeddedIdClass, ReflectionUtils.<Field> withAnnotation(Order.class));
		List<Integer> reversedFields = new LinkedList<Integer>();
		for (Field candidateField : candidateFields) {
			Order orderAnnotation = candidateField.getAnnotation(Order.class);
			if (orderAnnotation.reversed()) {
				reversedFields.add(orderAnnotation.value());
			}
		}
		Validator.validateBeanMappingTrue(reversedFields.size() <= 1,
				"There should be at most 1 field annotated with @Order(reversed=true) for the @EmbeddedId class '%s'",
				embeddedIdClass.getCanonicalName());
		return reversedFields.size() > 0 ? reversedFields.get(0) : null;

	}

	private int validateNoDuplicateOrderAndType(String embeddedIdClassName, Set<Integer> orders, int orderSum,
			int order, Class<?> componentType) {
        log.debug("Validate type and component ordering for embedded id class {} ", embeddedIdClassName);
		Validator.validateBeanMappingTrue(orders.add(order), "The order '%s' is duplicated in @EmbeddedId class '%s'",
				order, embeddedIdClassName);

		orderSum += order;

		PropertyParsingValidator.validateAllowedTypes(componentType, PropertyHelper.allowedTypes, "The class '"
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
		log.debug("Validate composite partiton key component ordering for @EmbeddedId class {} ", embeddedIdClassName);
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
		Validator.validateBeanMappingTrue(orderSum == check,
				"The composite partition key ordering is wrong for @EmbeddedId class '%s'", embeddedIdClassName);
	}

	private void validateReversedClusteredKey(Map<Integer, Field> componentsOrdering, Integer reversedField,
			String embeddedIdClassName) {
		if (reversedField != null) {
			log.debug("Validate reversed clustered key component ordering for @EmbeddedId class {} ",
					embeddedIdClassName);
			int lastPartitionKey = 0;
			for (Integer order : componentsOrdering.keySet()) {
				Field componentField = componentsOrdering.get(order);
				if (filter.hasAnnotation(componentField, PartitionKey.class)) {
					lastPartitionKey = Math.max(lastPartitionKey, order.intValue());
				}
			}
			if (lastPartitionKey > 0) {
				Validator
						.validateBeanMappingTrue(
								reversedField.intValue() == lastPartitionKey + 1,
								"The reversed clustered key must be set after the last partition key for @EmbeddedId class '%s'",
								embeddedIdClassName);
			} else {
				Validator.validateBeanMappingTrue(reversedField.intValue() == 2,
						"The composite clustered key must be set at position 2 for @EmbeddedId class '%s'",
						embeddedIdClassName);
			}
		}

	}

	private EmbeddedIdProperties buildComponentMetas(Class<?> embeddedIdClass, Map<Integer, Field> components,
                                                     Integer reversedField) {

        log.debug("Build components meta data for embedded id class {}",embeddedIdClass.getCanonicalName());
		EmbeddedIdPropertiesBuilder partitionKeysBuilder = new EmbeddedIdPropertiesBuilder();
		EmbeddedIdPropertiesBuilder clusteringKeysBuilder = new EmbeddedIdPropertiesBuilder();
		EmbeddedIdPropertiesBuilder embeddedIdPropertiesBuilder = new EmbeddedIdPropertiesBuilder();

		boolean hasPartitionKeyAnnotation = buildPartitionAndClusteringKeys(embeddedIdClass, components, reversedField,
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
			Integer reversedField, EmbeddedIdPropertiesBuilder partitionKeysBuilder,
			EmbeddedIdPropertiesBuilder clusteringKeysBuilder, EmbeddedIdPropertiesBuilder embeddedIdPropertiesBuilder) {
        log.debug("Build Components meta data for embedded id class {}",embeddedIdClass.getCanonicalName());

        boolean hasPartitionKeyAnnotation = false;
		for (Integer order : components.keySet()) {
			Field compositeKeyField = components.get(order);
			Class<?> componentClass = compositeKeyField.getType();
			String componentName;
			Method componentGetter = entityIntrospector.findGetter(embeddedIdClass, compositeKeyField);
			Method componentSetter = entityIntrospector.findSetter(embeddedIdClass, compositeKeyField);

			Column column = compositeKeyField.getAnnotation(Column.class);

			if (column != null && isNotBlank(column.name()))
				componentName = column.name();
			else
				componentName = compositeKeyField.getName();

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
			if (reversedField != null && order.intValue() == reversedField.intValue()) {
				clusteringKeysBuilder.setReversedComponentName(componentName);
			}
		}
		return hasPartitionKeyAnnotation;
	}

	private void checkForDefaultConstructor(Class<?> embeddedIdClass) {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		Set<Constructor> defaultConstructors = getAllConstructors(embeddedIdClass, withParametersCount(0));

		Validator.validateBeanMappingFalse(defaultConstructors.isEmpty(),
				"The @EmbeddedId class '%s' should have a public default constructor",
				embeddedIdClass.getCanonicalName());

	}
}
