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
package info.archinnov.achilles.internal.metadata.parsing.validator;

import static info.archinnov.achilles.type.ConsistencyLevel.ANY;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;
import info.archinnov.achilles.type.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.helper.PropertyHelper;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.internal.validation.Validator;

public class PropertyParsingValidator {
	private static final Logger log = LoggerFactory.getLogger(PropertyParsingValidator.class);

	public void validateNoDuplicate(PropertyParsingContext context) {
		String propertyName = context.getCurrentPropertyName();
		log.debug("Validate that property name {} is unique for the entity class {}", propertyName, context
				.getCurrentEntityClass().getCanonicalName());

		Validator.validateBeanMappingFalse(context.getPropertyMetas().containsKey(propertyName),
				"The property '%s' is already used for the entity '%s'", propertyName, context.getCurrentEntityClass()
						.getCanonicalName());

	}

	public void validateMapGenerics(Field field, Class<?> entityClass) {
		log.debug("Validate parameterized types for property {} of entity class {}", field.getName(),
				entityClass.getCanonicalName());

		Type genericType = field.getGenericType();
		if (!(genericType instanceof ParameterizedType)) {
			throw new AchillesBeanMappingException("The Map type should be parameterized for the entity '"
					+ entityClass.getCanonicalName() + "'");
		} else {
			ParameterizedType pt = (ParameterizedType) genericType;
			Type[] actualTypeArguments = pt.getActualTypeArguments();
			if (actualTypeArguments.length <= 1) {
				throw new AchillesBeanMappingException(
						"The Map type should be parameterized with <K,V> for the entity '"
								+ entityClass.getCanonicalName() + "'");
			}
		}
	}

	public void validateConsistencyLevelForCounter(PropertyParsingContext context,
			Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
		log.debug("Validate that counter property {} of entity class {} does not have ANY consistency level",
				context.getCurrentPropertyName(), context.getCurrentEntityClass().getCanonicalName());

		if (consistencyLevels.left == ANY || consistencyLevels.right == ANY) {
			throw new AchillesBeanMappingException(
					"Counter field '"
							+ context.getCurrentField().getName()
							+ "' of entity '"
							+ context.getCurrentEntityClass().getCanonicalName()
							+ "' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
		}
	}

	public void validateIndexIfSet(PropertyParsingContext context) {
		String fieldName = context.getCurrentPropertyName();
		String className = context.getCurrentEntityClass().getCanonicalName();
		log.debug("Validate that this property {} of entity class {} has a properly set index parameter, if set",
				fieldName, className);
		PropertyHelper propertyHelper = new PropertyHelper();
		if (propertyHelper.getIndexName(context.getCurrentField()) != null) {

			Validator.validateBeanMappingTrue(PropertyHelper.isSupportedType(context.getCurrentField().getType()),
					"Property '%s' of entity '%s' cannot be indexed because the type '%s' is not supported", fieldName,
					className, context.getCurrentField().getType().getCanonicalName());
			Validator.validateBeanMappingFalse(context.isEmbeddedId(),
					"Property '%s' of entity '%s' is part of a compound primary key and therefore cannot be indexed",
					fieldName, className);

			Validator.validateBeanMappingFalse(context.isPrimaryKey(),
					"Property '%s' of entity '%s' is a primary key and therefore cannot be indexed", fieldName,
					className);
		}
	}

	public static void validateAllowedTypes(Class<?> type, Set<Class<?>> allowedTypes, String message) {
        log.debug("Validate that type {} is supported",type);
		if (!allowedTypes.contains(type) && !type.isEnum()) {
			throw new AchillesBeanMappingException(message);
		}
	}

}
