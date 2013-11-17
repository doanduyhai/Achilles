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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.validation.Validator;

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
