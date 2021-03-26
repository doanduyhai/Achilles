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

package info.archinnov.achilles.internals.runtime;

import static info.archinnov.achilles.internals.futures.FutureUtils.toCompletableFuture;
import static java.lang.String.format;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;

import info.archinnov.achilles.internals.cache.CacheKey;
import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.context.ConfigurationContext;
import info.archinnov.achilles.internals.factory.TupleTypeFactory;
import info.archinnov.achilles.internals.factory.UserTypeFactory;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.statements.StatementWrapper;
import info.archinnov.achilles.type.SchemaNameProvider;

public class RuntimeEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeEngine.class);

    public final StatementsCache cache;
    public final ConfigurationContext configContext;
    public final Session session;
    public final String currentKeyspace;
    public final ExecutorService executor;

    public TupleTypeFactory tupleTypeFactory;
    public UserTypeFactory userTypeFactory;

    public RuntimeEngine(ConfigurationContext configContext) {
        this.configContext = configContext;
        this.session = configContext.getSession();
        this.cache = configContext.getStatementsCache();
        this.currentKeyspace = configContext.getCurrentKeyspace().orElseGet(session::getLoggedKeyspace);
        this.executor = configContext.getExecutorService();
    }

    public PreparedStatement getStaticCache(CacheKey cacheKey) {
        return cache.getStaticCache(cacheKey);
    }

    public CompletableFuture<ResultSet> execute(StatementWrapper wrapper) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Executing statement %s", wrapper.getBoundStatement().preparedStatement().getQueryString()));
        }

        wrapper.logDML();
        return toCompletableFuture(session.executeAsync(wrapper.getBoundStatement()), executor);
    }

    public CompletableFuture<ResultSet> execute(BoundStatement boundStatement) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Executing bound statement %s", boundStatement.preparedStatement().getQueryString()));
        }
        return toCompletableFuture(session.executeAsync(boundStatement), executor);
    }

    public CompletableFuture<ResultSet> execute(BatchStatement batchStatement) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Executing batch statement : %s",
                    batchStatement.getStatements()
                            .stream()
                            .map(Statement::toString)
                            .reduce("", (x, y) -> x + y)));
        }
        return toCompletableFuture(session.executeAsync(batchStatement), executor);
    }

    public PreparedStatement prepareDynamicQuery(RegularStatement statement) {
        return prepareDynamicQuery(statement.getQueryString());
    }

    public PreparedStatement prepareDynamicQuery(String queryString) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Preparing dynamic query %s", queryString));
        }
        return cache.getDynamicCache(queryString, session);
    }

    public Optional<PreparedStatement> maybePrepareIfDifferentSchemaNameFromCache(AbstractEntityProperty<?> entityProperty,
                                                                                  PreparedStatement psFromCache,
                                                                                  Optional<SchemaNameProvider> schemaNameProvider,
                                                                                  Supplier<RegularStatement> lambda) {
        if (schemaNameProvider.isPresent()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Preparing statement %s using optional schema name provider %s",
                        psFromCache.getQueryString(), schemaNameProvider));
            }
            final SchemaNameProvider provider = schemaNameProvider.get();
            final String tableNameWithKeyspace = provider.keyspaceFor(entityProperty.entityClass) + "." + provider.tableNameFor(entityProperty.entityClass);
            if (!psFromCache.getQueryString().toLowerCase().contains("from " + tableNameWithKeyspace)) {
                return Optional.of(prepareDynamicQuery(lambda.get()));
            }
        }
        return Optional.empty();

    }

    public Cluster getCluster() {
        return session.getCluster();
    }
}
