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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.LoaderImpl;
import info.archinnov.achilles.validation.Validator;

public class EntityLoader {

    private static final Logger log  = LoggerFactory.getLogger(EntityLoader.class);

    private LoaderImpl loaderImpl = new LoaderImpl();

	public <T> T load(PersistenceContext context, Class<T> entityClass) {
        log.debug("Loading entity of class {} using PersistenceContext {}",entityClass,context);
		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity '%s' key should not be null", entityClass.getCanonicalName());
		Validator
				.validateNotNull(entityMeta, "Entity meta for '%s' should not be null", entityClass.getCanonicalName());

		T entity;

		if (context.isLoadEagerFields()) {
			entity = loaderImpl.eagerLoadEntity(context);
		} else {
			entity = entityMeta.instanciate();
		}
		entityMeta.getIdMeta().setValueToField(entity, primaryKey);

		return entity;
	}

	public void loadPropertyIntoObject(PersistenceContext context, Object realObject, PropertyMeta pm) {
        log.trace("Loading property {} into object {}",pm.getPropertyName(),realObject);
		PropertyType type = pm.type();
		if (!type.isCounter()) {
			loaderImpl.loadPropertyIntoEntity(context, pm, realObject);
		}
	}

}
