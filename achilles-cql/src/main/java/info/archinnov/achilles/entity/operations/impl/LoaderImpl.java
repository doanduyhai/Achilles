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
package info.archinnov.achilles.entity.operations.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

import com.datastax.driver.core.Row;

public class LoaderImpl {
    private static final Logger log  = LoggerFactory.getLogger(LoaderImpl.class);

    private EntityMapper mapper = new EntityMapper();

	public <T> T eagerLoadEntity(PersistenceContext context) {
        log.trace("Loading entity using PersistenceContext {}",context);
		EntityMeta entityMeta = context.getEntityMeta();

		T entity = null;

		if (entityMeta.isClusteredCounter()) {
			PropertyMeta counterMeta = entityMeta.getFirstMeta();
			ConsistencyLevel readLevel = context.getConsistencyLevel().isPresent() ? context.getConsistencyLevel()
					.get() : counterMeta.getReadConsistencyLevel();
			Long counterValue = context.getClusteredCounter(counterMeta, readLevel);
			if (counterValue != null) {
				entity = entityMeta.instanciate();
			}
		} else {
			Row row = context.eagerLoadEntity();
			if (row != null) {
				entity = entityMeta.instanciate();
				mapper.setEagerPropertiesToEntity(row, entityMeta, entity);
			}
		}
		return entity;
	}

	public void loadPropertyIntoEntity(PersistenceContext context, PropertyMeta pm, Object entity) {
		Row row = context.loadProperty(pm);
		mapper.setPropertyToEntity(row, pm, entity);
	}
}
