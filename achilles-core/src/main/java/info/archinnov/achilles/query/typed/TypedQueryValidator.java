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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import info.archinnov.achilles.internal.statement.StatementHelper;
import info.archinnov.achilles.query.cql.NativeQueryValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMeta.GET_CQL_COLUMN_NAME;
import static info.archinnov.achilles.internal.metadata.holder.PropertyMeta.TO_LOWER_CASE;

public class TypedQueryValidator {
    private static final Logger log  = LoggerFactory.getLogger(TypedQueryValidator.class);

    private static String OPTIONAL_KEYSPACE_PREFIX = "[a-zA-Z0-9_]*\\.?";

    private NativeQueryValidator validator = NativeQueryValidator.Singleton.INSTANCE.get();

    public void validateTypedQuery(Class<?> entityClass, Statement statement, EntityMeta meta) {
        log.debug("Validate typed query {}", statement);
        String normalizedQueryString = StatementHelper.maybeGetNormalizedQueryString(statement);

        Validator.validateFalse(statement instanceof BatchStatement,"Cannot perform typed query with batch statements");

		PropertyMeta idMeta = meta.getIdMeta();

		validateRawTypedQuery(entityClass, statement, meta);

		if (!normalizedQueryString.contains("select *")) {

            final List<String> staticColumns = from(meta.getAllStaticMetas()).transform(GET_CQL_COLUMN_NAME).transform(TO_LOWER_CASE).toList();
			idMeta.forTypedQuery().validateTypedQuery(normalizedQueryString, staticColumns);
		}
	}

	public void validateRawTypedQuery(Class<?> entityClass, Statement statement, EntityMeta meta) {
        log.debug("Validate raw typed query {}",statement);
        String queryString = StatementHelper.maybeGetQueryString(statement);
        String tableName = meta.config().getTableName().toLowerCase();
        String normalizedQuery = queryString.toLowerCase();

        validator.validateSelect(statement);

        final Pattern pattern = Pattern.compile(".* from "+ OPTIONAL_KEYSPACE_PREFIX + tableName+"(?: )?.*");

        Validator.validateTrue(pattern.matcher(normalizedQuery).matches(),"The typed query [%s] should contain the table name '%s' if type is '%s'", queryString, tableName,
				entityClass.getCanonicalName());
	}

    public enum Singleton {
        INSTANCE;

        private final TypedQueryValidator instance = new TypedQueryValidator();

        public TypedQueryValidator get() {
            return instance;
        }
    }
}
