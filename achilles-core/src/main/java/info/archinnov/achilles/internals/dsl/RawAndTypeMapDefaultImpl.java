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

import static java.lang.String.format;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundStatementWrapper;
import info.archinnov.achilles.internals.statements.StatementWrapper;
import info.archinnov.achilles.internals.types.TypedMapIteratorWrapper;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;

public interface RawAndTypeMapDefaultImpl extends TypedMapAware, StatementTypeAware {

    RuntimeEngine runtimeEngine();

    AbstractEntityProperty<?> meta();

    BoundStatement boundStatement();

    Object[] encodedBoundValues();

    CassandraOptions options();

    @Override
    default CompletableFuture<Tuple2<List<TypedMap>, ExecutionInfo>> getTypedMapsAsyncWithStats() {
        final StatementWrapper statementWrapper = new BoundStatementWrapper(getOperationType(boundStatement()),
                meta(), boundStatement(), encodedBoundValues());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Select async with execution info : %s",
                    statementWrapper.getBoundStatement().preparedStatement().getQueryString()));
        }
        CompletableFuture<ResultSet> futureRS = runtimeEngine().execute(statementWrapper);

        return futureRS
                .thenApply(options()::resultSetAsyncListener)
                .thenApply(x -> statementWrapper.logReturnResults(x, options().computeMaxDisplayedResults(runtimeEngine().configContext)))
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> Tuple2.of(mapResultSetToTypedMaps(x), x.getExecutionInfo()));
    }

    @Override
    default CompletableFuture<Tuple2<TypedMap, ExecutionInfo>> getTypedMapAsyncWithStats() {
        final StatementWrapper statementWrapper = new BoundStatementWrapper(getOperationType(boundStatement()),
                meta(), boundStatement(), encodedBoundValues());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Execute native query async with execution info : %s",
                    statementWrapper.getBoundStatement().preparedStatement().getQueryString()));
        }

        CompletableFuture<ResultSet> cfutureRS = runtimeEngine().execute(statementWrapper);

        return cfutureRS
                .thenApply(options()::resultSetAsyncListener)
                .thenApply(x -> statementWrapper.logReturnResults(x, options().computeMaxDisplayedResults(runtimeEngine().configContext)))
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> Tuple2.of(mapRowToTypedMap(x.one()), x.getExecutionInfo()));
    }

    @Override
    default Iterator<TypedMap> typedMapIterator() {
        StatementWrapper statementWrapper = new BoundStatementWrapper(getOperationType(boundStatement()),
                meta(), boundStatement(), encodedBoundValues());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Generate iterator for typed query : %s",
                    statementWrapper.getBoundStatement().preparedStatement().getQueryString()));
        }

        CompletableFuture<ResultSet> futureRS = runtimeEngine().execute(statementWrapper);

        return new TypedMapIteratorWrapper(futureRS, statementWrapper, options());
    }

    @Override
    default Tuple2<Iterator<TypedMap>, ExecutionInfo> typedMapIteratorWithExecutionInfo() {
        TypedMapIteratorWrapper iterator = (TypedMapIteratorWrapper) this.typedMapIterator();
        return Tuple2.of(iterator, iterator.getExecutionInfo());
    }
}
