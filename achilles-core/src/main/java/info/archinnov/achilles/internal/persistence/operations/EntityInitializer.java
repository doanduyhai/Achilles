/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.persistence.operations;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;

public class EntityInitializer {
	private static final Logger log = LoggerFactory.getLogger(EntityInitializer.class);

	public <T> void initializeEntity(T entity, EntityMeta entityMeta) {

		log.debug("Initializing lazy fields for entity {} of class {}", entity, entityMeta.getClassName());

		for (PropertyMeta propertyMeta : entityMeta.getAllCounterMetas()) {
				propertyMeta.forValues().forceLoad(entity);
		}
	}

    public static enum Singleton {
        INSTANCE;

        private final EntityInitializer instance = new EntityInitializer();

        public EntityInitializer get() {
            return instance;
        }
    }
}
