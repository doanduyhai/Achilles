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
import static org.reflections.ReflectionUtils.*;
import info.archinnov.achilles.annotations.Order;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.parsing.validator.PropertyParsingValidator;
import info.archinnov.achilles.helper.EntityIntrospector;
import info.archinnov.achilles.helper.PropertyHelper;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;

import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompoundKeyParser {

	private static final Logger log = LoggerFactory
			.getLogger(PropertyHelper.class);
	protected EntityIntrospector entityIntrospector = new EntityIntrospector();

	public EmbeddedIdProperties parseEmbeddedId(Class<?> keyClass) {
		log.debug("Parse multikey class {} ", keyClass.getCanonicalName());

		List<Class<?>> componentClasses = new ArrayList<Class<?>>();
		List<String> componentNames = new ArrayList<String>();
		List<Method> componentGetters = new ArrayList<Method>();
		List<Method> componentSetters = new ArrayList<Method>();
		Map<Integer, Field> components = new HashMap<Integer, Field>();

		Constructor<?> constructor = scanAnnotatedFields(keyClass, components);
		Validator
				.validateBeanMappingTrue(
						components.size() > 1,
						"There should be at least 2 components for the @CompoundKey class '%s'",
						keyClass.getCanonicalName());

		EmbeddedIdProperties embeddedIdProperties = buildComponentMetas(
				keyClass, componentClasses, componentNames, componentGetters,
				componentSetters, components, constructor);

		log.trace("Built compound key properties : {}", embeddedIdProperties);
		return embeddedIdProperties;
	}

	private Constructor<?> scanAnnotatedFields(Class<?> keyClass,
			Map<Integer, Field> components) {

		@SuppressWarnings("unchecked")
		Set<Field> candidateFields = getFields(keyClass,
				ReflectionUtils.<Field> withAnnotation(Order.class));

		Set<Integer> orders = new HashSet<Integer>();
		int orderSum = 0;
		int componentCount = candidateFields.size();

		for (Field candidateField : candidateFields) {
			int order = candidateField.getAnnotation(Order.class).value();
			orderSum = validateNoDuplicateOrderAndType(keyClass, orders,
					orderSum, order, candidateField.getType());
			components.put(order, candidateField);
		}

		validateConsistentOrdering(keyClass, orderSum, componentCount);
		Validator
				.validateBeanMappingTrue(
						componentCount > 1,
						"There should be at least 2 fields annotated with @Order for the compound primary key class '%s'",
						keyClass.getCanonicalName());
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Set<Constructor> defaultConstructors = getAllConstructors(keyClass,
				withParametersCount(0));

		Validator
				.validateBeanMappingFalse(
						defaultConstructors.isEmpty(),
						"The @CompoundKey class '%s' should have a public default constructor",
						keyClass.getCanonicalName());

		return defaultConstructors.iterator().next();
	}

	// Shared methods
	private int validateNoDuplicateOrderAndType(Class<?> keyClass,
			Set<Integer> orders, int orderSum, int order, Class<?> componentType) {
		Validator.validateBeanMappingTrue(orders.add(order),
				"The order '%s' is duplicated in @CompoundKey class '%s'",
				order, keyClass.getCanonicalName());

		orderSum += order;

		PropertyParsingValidator
				.validateAllowedTypes(
						componentType,
						PropertyHelper.allowedTypes,
						"The class '"
								+ componentType.getCanonicalName()
								+ "' is not a valid component type for the @CompoundKey class '"
								+ keyClass.getCanonicalName() + "'");
		return orderSum;
	}

	private void validateConsistentOrdering(Class<?> keyClass, int orderSum,
			int keyCount) {
		int check = (keyCount * (keyCount + 1)) / 2;

		log.debug("Validate key ordering compound key class {} ",
				keyClass.getCanonicalName());

		Validator.validateBeanMappingTrue(orderSum == check,
				"The key orders is wrong for @CompoundKey class '%s'",
				keyClass.getCanonicalName());
	}

	private EmbeddedIdProperties buildComponentMetas(Class<?> keyClass,
			List<Class<?>> componentClasses, List<String> componentNames,
			List<Method> componentGetters, List<Method> componentSetters,
			Map<Integer, Field> components, Constructor<?> constructor) {

		List<Integer> orderList = new ArrayList<Integer>(components.keySet());
		Collections.sort(orderList);

		for (Integer order : orderList) {
			Field compoundKeyField = components.get(order);
			Column column = compoundKeyField.getAnnotation(Column.class);

			if (column != null && isNotBlank(column.name()))
				componentNames.add(column.name());
			else
				componentNames.add(compoundKeyField.getName());

			componentGetters.add(entityIntrospector.findGetter(keyClass,
					compoundKeyField));
			if (constructor.getParameterTypes().length == 0)
				componentSetters.add(entityIntrospector.findSetter(keyClass,
						compoundKeyField));

			componentClasses.add(compoundKeyField.getType());
		}

		Validator
				.validateBeanMappingNotEmpty(
						componentClasses,
						"No field or constructor param with @Order annotation found in the class '%s'",
						keyClass.getCanonicalName());

		EmbeddedIdProperties embeddedIdProperties = new EmbeddedIdProperties();
		embeddedIdProperties.setComponentClasses(componentClasses);
		embeddedIdProperties.setComponentNames(componentNames);
		embeddedIdProperties.setComponentGetters(componentGetters);
		embeddedIdProperties.setComponentSetters(componentSetters);

		return embeddedIdProperties;
	}
}
