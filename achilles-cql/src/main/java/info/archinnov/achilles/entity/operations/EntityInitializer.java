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

import static info.archinnov.achilles.entity.metadata.PropertyType.lazyType;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.util.AlreadyLoadedTransformer;
import info.archinnov.achilles.proxy.EntityInterceptor;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;

public class EntityInitializer {
	private static final Logger log = LoggerFactory.getLogger(EntityInitializer.class);

	private EntityProxifier proxifier = new EntityProxifier();

	public <T> void initializeEntity(T entity, EntityMeta entityMeta,
			EntityInterceptor<T> interceptor) {

		log.debug("Initializing lazy fields for entity {} of class {}", entity, entityMeta.getClassName());

		Set<PropertyMeta> alreadyLoadedMetas = FluentIterable.from(interceptor.getAlreadyLoaded())
				.transform(new AlreadyLoadedTransformer(entityMeta.getGetterMetas())).toImmutableSet();

		Set<PropertyMeta> allLazyMetas = FluentIterable.from(entityMeta.getPropertyMetas().values()).filter(lazyType)
				.toImmutableSet();

		Set<PropertyMeta> toBeLoadedMetas = Sets.difference(allLazyMetas, alreadyLoadedMetas);

		for (PropertyMeta propertyMeta : toBeLoadedMetas) {
			Object value = propertyMeta.getValueFromField(entity);
			if (propertyMeta.isCounter()) {
				Counter counter = (Counter) value;
				Object realObject = proxifier.getRealObject(entity);
				propertyMeta.setValueToField(realObject, CounterBuilder.incr(counter.get()));
			}
		}

		for (PropertyMeta propertyMeta : alreadyLoadedMetas) {
			if (propertyMeta.isCounter()) {
				Object value = propertyMeta.getValueFromField(entity);
				Counter counter = (Counter) value;
				Object realObject = proxifier.getRealObject(entity);
				propertyMeta.setValueToField(realObject, CounterBuilder.incr(counter.get()));
			}
		}
	}
}
