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

package info.archinnov.achilles.query.cql;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import info.archinnov.achilles.internal.validation.Validator;

public class NativeQueryValidator {

    public void validateUpsertOrDelete(RegularStatement regularStatement) {
        Validator.validateTrue(
                isUpsertStatement(regularStatement) || isDeleteStatement(regularStatement),
                "The statement '%s' should be an INSERT, an UPDATE or a DELETE",
                regularStatement.getQueryString()
        );
    }

    public void validateSelect(RegularStatement regularStatement) {
        Validator.validateTrue(isSelectStatement(regularStatement),"The statement '%s' should be a SELECT",regularStatement.getQueryString());
    }

    public boolean isSelectStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return regularStatement.getQueryString().toLowerCase().trim().startsWith("select ");
        } else {
            return regularStatement instanceof Select || regularStatement instanceof Select.Where;
        }
    }

    public boolean isInsertStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return regularStatement.getQueryString().toLowerCase().trim().startsWith("insert into ");
        } else {
            return regularStatement instanceof Insert || regularStatement instanceof Insert.Options;
        }
    }

    public boolean isUpdateStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return regularStatement.getQueryString().toLowerCase().trim().startsWith("update ");
        } else {
            return regularStatement instanceof Update.Where || regularStatement instanceof Update.Options;
        }
    }

    public boolean isDeleteStatement(RegularStatement regularStatement) {
        if (isSimpleStatement(regularStatement)) {
            return regularStatement.getQueryString().toLowerCase().trim().startsWith("delete ");
        } else {
            return regularStatement instanceof Delete.Where || regularStatement instanceof Delete.Options;
        }
    }

    public boolean isUpsertStatement(RegularStatement regularStatement) {
        return isInsertStatement(regularStatement) || isUpdateStatement(regularStatement);
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
