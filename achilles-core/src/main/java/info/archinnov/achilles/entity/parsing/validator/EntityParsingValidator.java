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
package info.archinnov.achilles.entity.parsing.validator;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityParsingValidator {
	private static final Logger log = LoggerFactory.getLogger(EntityParsingValidator.class);

	public void validateHasIdMeta(Class<?> entityClass, PropertyMeta idMeta) {
		log.debug("Validate that entity class {} has an id meta", entityClass.getCanonicalName());

		Validator.validateBeanMappingFalse(idMeta == null, "The entity '" + entityClass.getCanonicalName()
				+ "' should have at least one field with javax.persistence.Id/javax.persistence.EmbeddedId annotation");

	}

	public void validatePropertyMetas(EntityParsingContext context, PropertyMeta idMeta) {
		log.debug("Validate that there is at least one property meta for the entity class {}", context
				.getCurrentEntityClass().getCanonicalName());

		ArrayList<PropertyMeta> metas = new ArrayList<PropertyMeta>(context.getPropertyMetas().values());
		metas.remove(idMeta);
		Validator
				.validateBeanMappingFalse(
						metas.isEmpty(),
						"The entity '"
								+ context.getCurrentEntityClass().getCanonicalName()
								+ "' should have at least one field with javax.persistence.Column/javax.persistence.Id/javax.persistence.EmbeddedId annotations");

	}

	public void validateClusteredEntities(EntityParsingContext context) {
		Map<String, PropertyMeta> propertyMetas = context.getPropertyMetas();

		if (context.isClusteredEntity() && context.isThriftImpl()) {
			log.debug("Validate that there is at least one property meta for the clustered entity {}", context
					.getCurrentEntityClass().getCanonicalName());

			Validator.validateBeanMappingFalse(propertyMetas != null && propertyMetas.size() > 2,
					"The clustered entity '" + context.getCurrentEntityClass().getCanonicalName()
							+ "' should not have more than two properties annotated with @EmbeddedId/@Id/@Column");

			Iterator<Entry<String, PropertyMeta>> metaIter = propertyMetas.entrySet().iterator();
			PropertyType type1 = metaIter.next().getValue().type();

			log.debug("Validate that the clustered entity {} has an @EmbeddedId", context.getCurrentEntityClass()
					.getCanonicalName());

			Validator.validateBeanMappingTrue(type1 == PropertyType.EMBEDDED_ID,
					"The clustered entity '%s' should have an @EmbeddedId property", context.getCurrentEntityClass()
							.getCanonicalName());

			log.debug("Validate that the clustered entity {} has a valid clustered value type", context
					.getCurrentEntityClass().getCanonicalName());

			if (metaIter.hasNext()) {
				PropertyType type2 = metaIter.next().getValue().type();
				Validator.validateBeanMappingTrue(type2.isValidClusteredValueType(),
						"The clustered entity '%s' should have a single @Column property of type simple/counter",
						context.getCurrentEntityClass().getCanonicalName());
			}
		}
	}

	public void validateAtLeastOneEntity(List<Class<?>> entities, List<String> entityPackages) {
		log.debug("Validate that at least one entity is found in the packages {}",
				StringUtils.join(entityPackages, ","));

		Validator
				.validateBeanMappingFalse(
						entities.isEmpty(),
						"No entity with javax.persistence.Entity/javax.persistence.Table annotations found in the packages '%s'",
						StringUtils.join(entityPackages, ","));
	}
}
