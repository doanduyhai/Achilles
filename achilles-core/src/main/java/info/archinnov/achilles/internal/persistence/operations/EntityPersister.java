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
package info.archinnov.achilles.internal.persistence.operations;

import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.*;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.impl.PersisterImpl;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;

public class EntityPersister {
	private static final Logger log = LoggerFactory.getLogger(EntityPersister.class);

	private PersisterImpl persisterImpl = new PersisterImpl();

	public void persist(PersistenceContext context) {
		EntityMeta entityMeta = context.getEntityMeta();

		Object entity = context.getEntity();
		log.debug("Persisting transient entity {}", entity);

		if (entityMeta.isClusteredCounter()) {
			persisterImpl.persistClusteredCounter(context);
		} else {
			persistEntity(context, entityMeta);
		}
	}

	private void persistEntity(PersistenceContext context, EntityMeta entityMeta) {
		persisterImpl.persist(context);

		Set<PropertyMeta> counterMetas = from(entityMeta.getAllMetas()).filter(counterType).toImmutableSet();

		persisterImpl.persistCounters(context, counterMetas);
	}

	public void remove(PersistenceContext context) {
        log.debug("Deleting entity from PersistenceContext {}", context);
        persisterImpl.remove(context);
	}
}
