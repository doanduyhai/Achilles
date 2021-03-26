/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.dsl;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.type.SchemaNameProvider;
import info.archinnov.achilles.validation.Validator;

public interface SchemaNameAware {

    Logger LOGGER = LoggerFactory.getLogger(SchemaNameAware.class);

    default String lookupKeyspace(SchemaNameProvider provider, Class<?> entityClass) {

        final String foundKeyspace = provider.keyspaceFor(entityClass);
        Validator.validateNotBlank(foundKeyspace,
                "Keyspace found using schema name provider for entity class '%s' should not be blank/null",
                entityClass.getCanonicalName());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Lookup keyspace name for entity class %s using provider %s : %s",
                    entityClass.getCanonicalName(), provider, foundKeyspace));
        }
        return foundKeyspace;
    }

    default String lookupTable(SchemaNameProvider provider, Class<?> entityClass) {

        final String foundTableName = provider.tableNameFor(entityClass);
        Validator.validateNotBlank(foundTableName,
                "Table name found using schema name provider for entity class '%s' should not be blank/null",
                entityClass.getCanonicalName());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Lookup table name for entity class %s using provider %s : %s",
                    entityClass.getCanonicalName(), provider, foundTableName));
        }
        return foundTableName;
    }
}
