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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.types.ResultSetWrapper;

public class BoundStatementWrapper implements StatementWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoundStatementWrapper.class);

    private final OperationType operationType;
    private final AbstractEntityProperty<?> meta;
    private final Object[] boundValues;
    private final Object[] encodedBoundValues;
    private final Logger actualLogger;
    private BoundStatement bs;
    private UUID queryId;


    public BoundStatementWrapper(OperationType operationType, AbstractEntityProperty<?> meta, PreparedStatement ps,
                                 Object[] boundValues, Object[] encodedBoundValues) {
        this(operationType, meta, ps.bind(encodedBoundValues), boundValues, encodedBoundValues);
    }

    public BoundStatementWrapper(OperationType operationType, AbstractEntityProperty<?> meta,
                                 BoundStatement bs, Object[] encodedBoundValues) {
        this(operationType, meta, bs, new Object[]{}, encodedBoundValues);
    }

    public BoundStatementWrapper(OperationType operationType, AbstractEntityProperty<?> meta,
                                 BoundStatement bs, Object[] boundValues, Object[] encodedBoundValues) {
        this.operationType = operationType;
        this.meta = meta;
        this.bs = bs;
        this.boundValues = boundValues;
        this.encodedBoundValues = encodedBoundValues;
        this.actualLogger = meta.entityLogger.isDebugEnabled() ? meta.entityLogger : DML_LOGGER;
    }

    @Override
    public Object[] getBoundValues() {
        return boundValues;
    }

    @Override
    public BoundStatement getBoundStatement() {
        return bs;
    }

    @Override
    public void applyOptions(CassandraOptions cassandraOptions) {
        cassandraOptions.applyOptions(operationType, meta, bs);
    }

    @Override
    public void logDML() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Maybe log DML query %s", bs.preparedStatement().getQueryString()));
        }

        queryId = UUID.randomUUID();
        if (actualLogger.isDebugEnabled()) {
            writeDMLStatementLog(actualLogger, queryId, bs.preparedStatement().getQueryString(), bs.getConsistencyLevel(), boundValues, encodedBoundValues);
        }
    }

    @Override
    public ResultSet logReturnResults(ResultSet originalResultSet, int maxDisplayedRows) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Log returned results for query %s", bs.preparedStatement().getQueryString()));
        }

        if (actualLogger.isDebugEnabled()) {
            final ResultSetWrapper wrapper = new ResultSetWrapper(originalResultSet);
            logReturnedResultsInternal(actualLogger, queryId, wrapper, maxDisplayedRows);
            return wrapper;
        } else {
            return originalResultSet;
        }
    }

    @Override
    public Row logReturnedRow(Row row) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Log returned row for query %s", bs.preparedStatement().getQueryString()));
        }

        if (actualLogger.isDebugEnabled()) {
            logReturnedRowInternal(actualLogger, queryId, row);
        }
        return row;
    }

    @Override
    public ResultSet logTrace(ResultSet resultSet) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Maybe display tracing for query %s", bs.preparedStatement().getQueryString()));
        }
        tracingInternal(actualLogger, queryId, resultSet);
        return resultSet;
    }
}
