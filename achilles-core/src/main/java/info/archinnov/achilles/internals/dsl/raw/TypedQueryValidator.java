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

package info.archinnov.achilles.internals.dsl.raw;

import static info.archinnov.achilles.validation.Validator.validateTrue;
import static java.lang.String.format;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;

public class TypedQueryValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TypedQueryValidator.class);
    private static final String OPTIONAL_KEYSPACE_PREFIX = "[a-zA-Z0-9_]*\\.?";

    public static void validateCorrectTableName(String queryString, AbstractEntityProperty<?> meta) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Validate that the query string %s is related to the entity meta %s",
                    queryString, meta.toString()));
        }

        String tableName = meta.getTableOrViewName().toLowerCase();

        final Pattern pattern = Pattern.compile(".* from " + OPTIONAL_KEYSPACE_PREFIX + tableName + "(?: )?.*");
        validateTrue(pattern.matcher(queryString).matches(), "The typed query [%s] should contain the table name '%s' if the entity type is '%s'", queryString, tableName,
                meta.entityClass.getCanonicalName());
    }

}
