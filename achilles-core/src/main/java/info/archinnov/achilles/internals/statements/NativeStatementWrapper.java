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

package info.archinnov.achilles.internals.statements;

import static java.lang.String.format;

import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.types.ResultSetWrapper;

public class NativeStatementWrapper implements StatementWrapper {

    private final AbstractEntityProperty<?> meta;
    private final BoundStatement boundStatement;
    private final Object[] encodedBoundValues;
    private final UUID queryId = UUID.randomUUID();
    private final OperationType operationType;


    public NativeStatementWrapper(OperationType operationType, AbstractEntityProperty<?> meta, BoundStatement boundStatement, Object[] encodedBoundValues) {
        this.meta = meta;
        this.boundStatement = boundStatement;
        this.encodedBoundValues = encodedBoundValues;
        this.operationType = operationType;
    }

    @Override
    public Object[] getBoundValues() {
        return encodedBoundValues;
    }

    @Override
    public BoundStatement getBoundStatement() {
        return boundStatement;
    }

    @Override
    public void applyOptions(CassandraOptions cassandraOptions) {
        cassandraOptions.applyOptions(operationType, meta, boundStatement);
    }

    @Override
    public void logDML() {
        writeDMLStatementLog(DML_LOGGER, queryId,
                boundStatement.preparedStatement().getQueryString(),
                boundStatement.getConsistencyLevel(), new Object[0], encodedBoundValues);
    }

    @Override
    public ResultSet logReturnResults(ResultSet originalResultSet, int maxDisplayedRows) {
        if (DML_LOGGER.isDebugEnabled()) {
            final ResultSetWrapper wrapper = new ResultSetWrapper(originalResultSet);
            logReturnedResultsInternal(DML_LOGGER, queryId, wrapper, maxDisplayedRows);
            return wrapper;
        } else {
            return originalResultSet;
        }
    }

    @Override
    public Row logReturnedRow(Row row) {
        if (DML_LOGGER.isDebugEnabled()) {
            logReturnedRowInternal(DML_LOGGER, queryId, row);
        }
        return row;
    }

    @Override
    public ResultSet logTrace(ResultSet resultSet) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Maybe display tracing for query %s", boundStatement.preparedStatement().getQueryString()));
        }
        tracingInternal(DML_LOGGER, queryId, resultSet);
        return resultSet;
    }
}
