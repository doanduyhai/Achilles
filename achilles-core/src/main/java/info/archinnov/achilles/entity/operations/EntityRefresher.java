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

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.proxy.EntityInterceptor;

import java.lang.reflect.Method;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityRefresher<CONTEXT extends PersistenceContext> {
	private static final Logger log = LoggerFactory.getLogger(EntityRefresher.class);

	private EntityProxifier<CONTEXT> proxifier;
	private EntityLoader<CONTEXT> loader;

	public EntityRefresher() {
	}

	public EntityRefresher(EntityLoader<CONTEXT> loader, EntityProxifier<CONTEXT> proxifier) {
		this.loader = loader;
		this.proxifier = proxifier;
	}

	public <T> void refresh(CONTEXT context) throws AchillesStaleObjectStateException {
		Object primaryKey = context.getPrimaryKey();
		log.debug("Refreshing entity of class {} and primary key {}", context.getEntityClass().getCanonicalName(),
				primaryKey);

		Object entity = context.getEntity();

		EntityInterceptor<CONTEXT, Object> interceptor = proxifier.getInterceptor(entity);

		interceptor.getDirtyMap().clear();
		Set<Method> alreadyLoaded = interceptor.getAlreadyLoaded();
		alreadyLoaded.clear();
		alreadyLoaded.addAll(context.getEntityMeta().getEagerGetters());

		Object freshEntity = loader.load(context, context.getEntityClass());

		if (freshEntity == null) {
			throw new AchillesStaleObjectStateException("The entity '" + entity + "' with primary_key '" + primaryKey
					+ "' no longer exists in Cassandra");
		}
		interceptor.setTarget(freshEntity);
	}
}
