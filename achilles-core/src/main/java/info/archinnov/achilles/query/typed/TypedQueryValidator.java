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
package info.archinnov.achilles.query.typed;

import info.archinnov.achilles.internal.provider.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.query.cql.NativeQueryValidator;

import java.util.regex.Pattern;

public class TypedQueryValidator {
    private static final Logger log  = LoggerFactory.getLogger(TypedQueryValidator.class);

    private static String OPTIONAL_KEYSPACE_PREFIX = "[a-zA-Z0-9_]*\\.?";

    private NativeQueryValidator validator = NativeQueryValidator.Singleton.INSTANCE.get();

    public void validateTypedQuery(Class<?> entityClass, RegularStatement regularStatement, EntityMeta meta) {
        log.debug("Validate typed query {}",regularStatement.getQueryString());
		PropertyMeta idMeta = meta.getIdMeta();
		String queryString = regularStatement.getQueryString().toLowerCase().trim();

		validateRawTypedQuery(entityClass, regularStatement, meta);

		if (!queryString.contains("select *")) {
			idMeta.forTypedQuery().validateTypedQuery(queryString);
		}
	}

	public void validateRawTypedQuery(Class<?> entityClass, RegularStatement regularStatement, EntityMeta meta) {
        log.debug("Validate raw typed query {}",regularStatement);
        String tableName = meta.config().getTableName().toLowerCase();
        final String queryString = regularStatement.getQueryString();
        String normalizedQuery = queryString.toLowerCase();

        validator.validateSelect(regularStatement);

        final Pattern pattern = Pattern.compile(".* from "+ OPTIONAL_KEYSPACE_PREFIX + tableName+" .*");

        Validator.validateTrue(pattern.matcher(normalizedQuery).matches(),"The typed query [%s] should contain the table name '%s' if type is '%s'", queryString, tableName,
				entityClass.getCanonicalName());
	}

    public static enum Singleton {
        INSTANCE;

        private final TypedQueryValidator instance = new TypedQueryValidator();

        public TypedQueryValidator get() {
            return instance;
        }
    }
}
