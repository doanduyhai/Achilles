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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.validation.Validator;

import java.util.regex.Pattern;

public class NativeQueryValidator {

    private static final Pattern SELECT_PATTERN = Pattern.compile("select .*");
    private static final Pattern INSERT_PATTERN = Pattern.compile("insert .*");
    private static final Pattern UPDATE_PATTERN = Pattern.compile("update .*");
    private static final Pattern DELETE_PATTERN = Pattern.compile("delete .*");


    public void validateUpsertOrDelete(Statement statement) {
        if (statement instanceof RegularStatement) {
            final RegularStatement regularStatement = (RegularStatement) statement;
            Validator.validateTrue(
                    isUpsertStatement(regularStatement) || isDeleteStatement(regularStatement),
                    "The regular statement '%s' should be an INSERT, an UPDATE or a DELETE",
                    regularStatement.getQueryString()
            );
        } else if(statement instanceof BoundStatement) {
            final BoundStatement boundStatement = (BoundStatement) statement;
            Validator.validateTrue(
                    isUpsertStatement(boundStatement) || isDeleteStatement(boundStatement),
                    "The bound statement '%s' should be an INSERT, an UPDATE or a DELETE",
                    boundStatement.preparedStatement().getQueryString()
            );
        }
    }

    public void validateSelect(Statement statement) {
        if (statement instanceof RegularStatement) {
            final RegularStatement regularStatement = (RegularStatement) statement;
            Validator.validateTrue(isSelectStatement(regularStatement), "The regular statement '%s' should be a SELECT", regularStatement.getQueryString());
        } else if (statement instanceof BoundStatement) {
            final BoundStatement boundStatement = (BoundStatement) statement;
            Validator.validateTrue(isSelectStatement(boundStatement), "The bound statement '%s' should be a SELECT", boundStatement.preparedStatement().getQueryString());
        }
    }

    public boolean isSelectStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return SELECT_PATTERN.matcher(regularStatement.getQueryString().toLowerCase().trim()).matches();
        } else {
            return regularStatement instanceof Select || regularStatement instanceof Select.Where;
        }
    }

    public boolean isSelectStatement(BoundStatement boundStatement) {
        return SELECT_PATTERN.matcher(boundStatement.preparedStatement().getQueryString().toLowerCase().trim()).matches();
    }

    public boolean isInsertStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return INSERT_PATTERN.matcher(regularStatement.getQueryString().toLowerCase().trim()).matches();
        } else {
            return regularStatement instanceof Insert || regularStatement instanceof Insert.Options;
        }
    }

    public boolean isInsertStatement(BoundStatement boundStatement) {
        return INSERT_PATTERN.matcher(boundStatement.preparedStatement().getQueryString().toLowerCase().trim()).matches();
    }

    public boolean isUpdateStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return UPDATE_PATTERN.matcher(regularStatement.getQueryString().toLowerCase().trim()).matches();
        } else {
            return regularStatement instanceof Update.Where || regularStatement instanceof Update.Options;
        }
    }

    public boolean isUpdateStatement(BoundStatement boundStatement) {
        return UPDATE_PATTERN.matcher(boundStatement.preparedStatement().getQueryString().toLowerCase().trim()).matches();
    }

    public boolean isDeleteStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return DELETE_PATTERN.matcher(regularStatement.getQueryString().toLowerCase().trim()).matches();
        } else {
            return regularStatement instanceof Delete.Where || regularStatement instanceof Delete.Options;
        }
    }

    public boolean isDeleteStatement(BoundStatement boundStatement) {
        return DELETE_PATTERN.matcher(boundStatement.preparedStatement().getQueryString().toLowerCase().trim()).matches();
    }

    public boolean isUpsertStatement(RegularStatement regularStatement) {
        return isInsertStatement(regularStatement) || isUpdateStatement(regularStatement);
    }

    public boolean isUpsertStatement(BoundStatement boundStatement) {
        return isInsertStatement(boundStatement) || isUpdateStatement(boundStatement);
    }

    public boolean isSimpleStatement(RegularStatement regularStatement) {
        return regularStatement instanceof SimpleStatement;
    }

    public static enum Singleton {
        INSTANCE;

        private final NativeQueryValidator instance = new NativeQueryValidator();

        public NativeQueryValidator get() {
            return instance;
        }
    }
}
