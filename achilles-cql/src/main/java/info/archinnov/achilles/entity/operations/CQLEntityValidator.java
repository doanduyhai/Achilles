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
package info.archinnov.achilles.entity.operations;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;

public class CQLEntityValidator {
	private static final Logger log = LoggerFactory.getLogger(CQLEntityValidator.class);

	private CQLEntityProxifier proxifier = new CQLEntityProxifier();

	public void validateEntity(Object entity, Map<Class<?>, EntityMeta> entityMetaMap) {
		Validator.validateNotNull(entity, "Entity should not be null");

		Class<?> baseClass = proxifier.deriveBaseClass(entity);
		EntityMeta entityMeta = entityMetaMap.get(baseClass);
		validateEntity(entity, entityMeta);

	}

	public void validateEntity(Object entity, EntityMeta entityMeta) {
		log.debug("Validate entity {}", entity);
		Validator.validateNotNull(entityMeta, "The entity %s is not managed by Achilles", entity.getClass()
				.getCanonicalName());

		Object id = entityMeta.getPrimaryKey(entity);
		if (id == null) {
			throw new IllegalArgumentException("Cannot get primary key for entity "
					+ entity.getClass().getCanonicalName());
		}
		validatePrimaryKey(entityMeta.getIdMeta(), id);
	}

	public void validatePrimaryKey(PropertyMeta idMeta, Object primaryKey) {
		if (idMeta.isEmbeddedId()) {
			List<Object> components = idMeta.encodeToComponents(primaryKey);
			for (Object component : components) {
				Validator.validateNotNull(component, "The clustered key '%s' components should not be null",
						idMeta.getPropertyName());
			}
		}
	}

	public void validateNotClusteredCounter(Object entity, Map<Class<?>, EntityMeta> entityMetaMap) {
		Class<?> baseClass = proxifier.deriveBaseClass(entity);
		EntityMeta entityMeta = entityMetaMap.get(baseClass);
		Validator.validateFalse(entityMeta.isClusteredCounter(),
				"The entity '%s' is a clustered counter and does not support insert/update with TTL", entity);
	}
}
