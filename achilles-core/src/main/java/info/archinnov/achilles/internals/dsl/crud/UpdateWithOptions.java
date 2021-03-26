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

package info.archinnov.achilles.internals.dsl.crud;

import static info.archinnov.achilles.internals.dsl.LWTHelper.triggerLWTListeners;
import static info.archinnov.achilles.type.interceptor.Event.POST_UPDATE;
import static info.archinnov.achilles.type.interceptor.Event.PRE_UPDATE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import info.archinnov.achilles.internals.dsl.StatementProvider;
import info.archinnov.achilles.internals.dsl.action.MutationAction;
import info.archinnov.achilles.internals.dsl.options.AbstractOptionsForCRUDUpdate;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.runtime.RuntimeEngine;
import info.archinnov.achilles.internals.statements.BoundValuesWrapper;
import info.archinnov.achilles.internals.statements.PreparedStatementGenerator;
import info.archinnov.achilles.internals.statements.StatementWrapper;

public class UpdateWithOptions<ENTITY> extends AbstractOptionsForCRUDUpdate<UpdateWithOptions<ENTITY>>
        implements MutationAction, StatementProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateWithOptions.class);

    private final AbstractEntityProperty<ENTITY> meta;
    private final RuntimeEngine rte;
    private final ENTITY instance;
    private final CassandraOptions options;
    private final boolean updateStatic;

    public UpdateWithOptions(AbstractEntityProperty<ENTITY> meta, RuntimeEngine rte, ENTITY instance, boolean updateStatic, Optional<CassandraOptions> cassandraOptions) {
        this.meta = meta;
        this.rte = rte;
        this.instance = instance;
        this.updateStatic = updateStatic;
        this.options = cassandraOptions.orElse(new CassandraOptions());
    }

    public CompletableFuture<ExecutionInfo> executeAsyncWithStats() {

        meta.triggerInterceptorsForEvent(PRE_UPDATE, instance);


        final StatementWrapper statementWrapper = getInternalBoundStatementWrapper();
        final String queryString = statementWrapper.getBoundStatement().preparedStatement().getQueryString();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Insert async with execution info : %s", queryString));
        }

        CompletableFuture<ResultSet> cfutureRS = rte.execute(statementWrapper);

        return cfutureRS
                .thenApply(this.options::resultSetAsyncListener)
                .thenApply(statementWrapper::logTrace)
                .thenApply(x -> triggerLWTListeners(lwtResultListeners, x, queryString))
                .thenApply(x -> x.getExecutionInfo())
                .thenApply(x -> {
                    meta.triggerInterceptorsForEvent(POST_UPDATE, instance);
                    return x;
                });
    }

    @Override
    protected CassandraOptions getOptions() {
        return options;
    }

    @Override
    public BoundStatement generateAndGetBoundStatement() {
        return getInternalBoundStatementWrapper().getBoundStatement();
    }


    @Override
    public String getStatementAsString() {
        return getInternalPreparedStatement().getQueryString();
    }

    @Override
    public List<Object> getBoundValues() {
        BoundValuesWrapper wrapper = updateStatic == true
                ? meta.extractPartitionKeysAndStaticColumnsFromEntity(instance, options)
                : meta.extractAllValuesFromEntity(instance, options);
        return wrapper.boundValuesInfo.stream().map(x -> x.boundValue).collect(toList());
    }

    @Override
    public List<Object> getEncodedBoundValues() {
        BoundValuesWrapper wrapper = updateStatic == true
                ? meta.extractPartitionKeysAndStaticColumnsFromEntity(instance, options)
                : meta.extractAllValuesFromEntity(instance, options);
        return wrapper.boundValuesInfo.stream().map(x -> x.encodedValue).collect(toList());
    }

    @Override
    protected UpdateWithOptions<ENTITY> getThis() {
        return this;
    }

    private StatementWrapper getInternalBoundStatementWrapper() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get bound statement wrapper"));
        }

        final PreparedStatement ps = getInternalPreparedStatement();
        BoundValuesWrapper wrapper = updateStatic == true
                ? meta.extractPartitionKeysAndStaticColumnsFromEntity(instance, options)
                : meta.extractAllValuesFromEntity(instance, options);

        StatementWrapper statementWrapper = wrapper.bindForUpdate(ps);
        statementWrapper.applyOptions(options);
        return statementWrapper;
    }

    private PreparedStatement getInternalPreparedStatement() {
        return rte.prepareDynamicQuery(PreparedStatementGenerator.generateUpdate(instance, meta, options, updateStatic,
                (ifExists.isPresent() && ifExists.get() == true)));
    }


}
