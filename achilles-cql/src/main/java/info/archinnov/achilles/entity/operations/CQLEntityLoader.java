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

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.CQLLoaderImpl;
import info.archinnov.achilles.validation.Validator;

public class CQLEntityLoader implements EntityLoader<CQLPersistenceContext> {
	private CQLLoaderImpl loaderImpl = new CQLLoaderImpl();

	@Override
	public <T> T load(CQLPersistenceContext context, Class<T> entityClass) {
		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();

		Validator.validateNotNull(entityClass, "Entity class should not be null");
		Validator.validateNotNull(primaryKey, "Entity '%s' key should not be null", entityClass.getCanonicalName());
		Validator
				.validateNotNull(entityMeta, "Entity meta for '%s' should not be null", entityClass.getCanonicalName());

		T entity = null;

		if (context.isLoadEagerFields()) {
			entity = loaderImpl.eagerLoadEntity(context, entityClass);
		} else {
			entity = entityMeta.<T> instanciate();
		}
		entityMeta.getIdMeta().setValueToField(entity, primaryKey);

		return entity;
	}

	@Override
	public <V> void loadPropertyIntoObject(CQLPersistenceContext context, Object realObject, PropertyMeta pm) {
		PropertyType type = pm.type();
		if (!type.isCounter()) {
			loaderImpl.loadPropertyIntoEntity(context, pm, realObject);
		}
	}

}
