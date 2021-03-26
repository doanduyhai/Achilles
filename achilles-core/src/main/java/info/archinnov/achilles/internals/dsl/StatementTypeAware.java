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


import static info.archinnov.achilles.internals.statement.StatementHelper.*;
import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Statement;

import info.archinnov.achilles.internals.statements.OperationType;

public interface StatementTypeAware {

    Logger LOGGER = LoggerFactory.getLogger(StatementTypeAware.class);

    default OperationType getOperationType(Statement statement) {
        OperationType foundType;
        if (isSelectStatement(statement)) {
            foundType = OperationType.SELECT;
        } else if (isInsertStatement(statement)) {
            foundType = OperationType.INSERT;
        } else if (isUpdateStatement(statement)) {
            foundType = OperationType.UPDATE;
        } else if (isBatchStatement(statement)) {
            foundType = OperationType.OTHER;
        } else {
            foundType = OperationType.OTHER;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Operation type found for statement %s : %s",
                    statement.toString(), foundType.name()));
        }
        return foundType;
    }
}
