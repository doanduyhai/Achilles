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
package info.archinnov.achilles.internal.metadata.parsing.validator;

import static info.archinnov.achilles.internal.metadata.holder.PropertyMeta.COUNTER_COLUMN_FILTER;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMeta.STATIC_COLUMN_FILTER;
import static info.archinnov.achilles.internal.validation.Validator.validateBeanMappingTrue;
import java.util.Collection;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

public class EntityParsingValidator {
	private static final Logger log = LoggerFactory.getLogger(EntityParsingValidator.class);

	public void validateHasIdMeta(Class<?> entityClass, PropertyMeta idMeta) {
		log.debug("Validate that entity class {} has an id meta", entityClass.getCanonicalName());

		Validator.validateBeanMappingFalse(idMeta == null, "The entity '" + entityClass.getCanonicalName()
				+ "' should have at least one field with info.archinnov.achilles.annotations.Id/info.archinnov.achilles.annotations.EmbeddedId annotation");

	}

    public void validateStaticColumns(EntityMeta entityMeta,PropertyMeta idMeta) {
        final String className = entityMeta.getClassName();
        final Collection<PropertyMeta> propertyMetas = entityMeta.getPropertyMetas().values();
        final boolean hasStaticColumns = FluentIterable.from(propertyMetas).filter(STATIC_COLUMN_FILTER).size()>0;
        if (hasStaticColumns) {
            validateBeanMappingTrue(idMeta.isClustered(), "The entity class '%s' cannot have a static column because it does not declare any clustering column", className);
        }

        final boolean hasStaticCounters = FluentIterable.from(propertyMetas).filter(STATIC_COLUMN_FILTER).filter(COUNTER_COLUMN_FILTER).size()>0;
        if (hasStaticCounters) {
            Validator.validateBeanMappingTrue(entityMeta.isClusteredCounter(),"The entity class '%s' cannot have a static counter column if it is not a clustered entity with only counter properties", className);
        }
    }
}
