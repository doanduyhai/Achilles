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

package info.archinnov.achilles.internals.statement;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Helper class to determine statement types: SELECT, INSERT, UPDATE, DELETE or OTHER
 */
public class StatementHelper {

    public static final String BEGIN_BATCH = "begin batch";
    public static final String OTHER_STATEMENT = "other statement";
    private static final Logger LOGGER = LoggerFactory.getLogger(StatementHelper.class);
    private static final Pattern SELECT_PATTERN = Pattern.compile("select .*");
    private static final Pattern INSERT_PATTERN = Pattern.compile("insert .*");
    private static final Pattern UPDATE_PATTERN = Pattern.compile("update .*");
    private static final Pattern DELETE_PATTERN = Pattern.compile("delete .*");
    private static final Pattern BATCH_PATTERN = Pattern.compile("begin\\w*batch .*");

    public static String maybeGetNormalizedQueryString(Statement statement) {
        return normalizeQueryString(maybeGetQueryString(statement));
    }

    public static String normalizeQueryString(String queryString) {
        return queryString.toLowerCase().trim().replaceAll("\n", "");
    }

    public static boolean isSelectStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Select statement ? ", statement.toString()));
        }
        return SELECT_PATTERN.matcher(maybeGetNormalizedQueryString(statement)).matches();
    }

    public static boolean isSelectStatement(PreparedStatement preparedStatement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Select statement ? ", preparedStatement.getQueryString()));
        }
        final String queryString = preparedStatement.getQueryString().toLowerCase().trim().replaceAll("\n", "");
        return SELECT_PATTERN.matcher(queryString).matches();
    }

    public static boolean isInsertStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Insert statement ? ", statement.toString()));
        }
        if (isSimpleStatement(statement)) {
            return INSERT_PATTERN.matcher(maybeGetNormalizedQueryString(statement)).matches();
        } else {
            return statement instanceof Insert ||
                    statement instanceof Insert.Options;
        }
    }


    public static boolean isUpdateStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Update statement ? ", statement.toString()));
        }
        if (isSimpleStatement(statement)) {
            return UPDATE_PATTERN.matcher(maybeGetNormalizedQueryString(statement)).matches();
        } else {
            return statement instanceof Update.Where || statement instanceof Update.Options;
        }
    }

    public static boolean isDeleteStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Delete statement ? ", statement.toString()));
        }
        if (isSimpleStatement(statement)) {
            return DELETE_PATTERN.matcher(maybeGetNormalizedQueryString(statement)).matches();
        } else {
            return statement instanceof Delete.Where || statement instanceof Delete.Options;
        }
    }

    public static boolean isBatchStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Batch statement ? ", statement.toString()));
        }
        return statement instanceof BatchStatement ||
                BATCH_PATTERN.matcher(maybeGetNormalizedQueryString(statement)).matches();
    }

    public static boolean isUpsertStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' Upsert statement ? ", statement.toString()));
        }
        return isInsertStatement(statement) || isUpdateStatement(statement) || isBatchStatement(statement);
    }

    public static boolean isSimpleStatement(Statement statement) {
        return statement instanceof SimpleStatement;
    }

    public static boolean isDMLStatement(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Is '%s' DML statement ? ", statement.toString()));
        }
        return isSelectStatement(statement)
                || isUpsertStatement(statement)
                || isDeleteStatement(statement);
    }

    private static String maybeGetQueryString(Statement statement) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Maybe get query string from %s ", statement.toString()));
        }

        if (statement instanceof RegularStatement) {
            return ((RegularStatement) statement).getQueryString();
        } else if (statement instanceof BoundStatement) {
            return ((BoundStatement) statement).preparedStatement().getQueryString();
        } else if (statement instanceof BatchStatement) {
            return BEGIN_BATCH;
        } else if (statement instanceof StatementWrapper) {
            return maybeGetQueryString(StatementWrapperUtils.getWrappedStatement(((StatementWrapper) statement)));
        } else {
            return OTHER_STATEMENT;
        }
    }
}
