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

package info.archinnov.achilles.internal.statement.wrapper;

import static info.archinnov.achilles.internal.consistency.ConsistencyConverter.getCQLLevel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.type.ConsistencyLevel;

public class BatchStatementWrapper extends AbstractStatementWrapper {

    private BatchStatement.Type batchType;
    private List<AbstractStatementWrapper> statementWrappers;
    private Optional<ConsistencyLevel> consistencyLevelO;
    private Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevelO;
    private BatchStatement batchStatement;
    private CompositeCASResultListener compositeCASResultListener;

    public BatchStatementWrapper(BatchStatement.Type batchType, List<AbstractStatementWrapper> statementWrappers,
            Optional<ConsistencyLevel> consistencyLevelO, Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyLevel) {
        super(null, null);
        this.batchType = batchType;
        this.statementWrappers = statementWrappers;
        this.consistencyLevelO = consistencyLevelO;
        this.serialConsistencyLevelO = serialConsistencyLevel;
        this.compositeCASResultListener = new CompositeCASResultListener();
        super.casResultListener = Optional.<CASResultListener>fromNullable(this.compositeCASResultListener);
        this.batchStatement = createBatchStatement(batchType, statementWrappers);
    }

    private BatchStatement createBatchStatement(BatchStatement.Type batchType, List<AbstractStatementWrapper> statementWrappers) {
        BatchStatement batch = new BatchStatement(batchType);
        boolean tracingEnabled = false;
        for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            statementWrapper.activateQueryTracing();
            tracingEnabled |= statementWrapper.isTracingEnabled();

            if (statementWrapper.casResultListener.isPresent()) {
                this.compositeCASResultListener.addCASResultListener(statementWrapper.casResultListener.get());
            }

            if (statementWrapper instanceof NativeStatementWrapper) {
                batch.add(((NativeStatementWrapper) statementWrapper).buildParameterizedStatement());
            } else {
                batch.add(statementWrapper.getStatement());
            }
        }
        if (tracingEnabled) {
            batch.enableTracing();
        }
        if (consistencyLevelO.isPresent()) {
            batch.setConsistencyLevel(getCQLLevel(consistencyLevelO.get()));
        }
        // TODO Serial Consistency not supported for batch. Wait for C* 2.1
        if (serialConsistencyLevelO.isPresent()) {
           // batch.setSerialConsistencyLevel(serialConsistencyLevelO.get());
        }
        return batch;
    }

    @Override
    public String getQueryString() {
        List<String> queries = new ArrayList<>();
        for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            queries.add(statementWrapper.getQueryString());
        }
        return Joiner.on("\n").join(queries);
    }

    @Override
    public ListenableFuture<ResultSet> executeAsync(Session session, ExecutorService executorService) {
        return super.executeAsyncInternal(session, this, executorService);
    }

    @Override
    public Statement getStatement() {
        return this.batchStatement;
    }

    @Override
    public void logDMLStatement(String indentation) {
        if (dmlLogger.isDebugEnabled() || batchStatement.isTracing()) {
            AbstractStatementWrapper.writeDMLStartBatch(batchType);
        }

        for (AbstractStatementWrapper statementWrapper : statementWrappers) {
            statementWrapper.logDMLStatement(indentation);
        }
        if (dmlLogger.isDebugEnabled() || batchStatement.isTracing()) {
            ConsistencyLevel consistencyLevel = consistencyLevelO.isPresent() ? consistencyLevelO.get() : null;
            AbstractStatementWrapper.writeDMLEndBatch(batchType, consistencyLevel);
        }
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevelO.get();
    }

    static class CompositeCASResultListener implements CASResultListener {

        private final Set<CASResultListener> delegates = new HashSet<>();

        private void addCASResultListener(CASResultListener listener) {
            delegates.add(listener);
        }

        @Override
        public void onCASSuccess() {
            for (CASResultListener listener : delegates) {
                listener.onCASSuccess();
            }
        }

        @Override
        public void onCASError(CASResult casResult) {
            for (CASResultListener listener : delegates) {
                listener.onCASError(casResult);
            }
        }
    }
}
