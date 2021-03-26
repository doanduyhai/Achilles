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

package info.archinnov.achilles.junit;

import static info.archinnov.achilles.junit.AchillesTestResource.Steps.BOTH;
import static info.archinnov.achilles.validation.Validator.validateTrue;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServer;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.internals.cache.StatementsCache;
import info.archinnov.achilles.internals.runtime.AbstractManagerFactory;
import info.archinnov.achilles.logger.AchillesLoggers;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;

/**
 * <strong>WARNING: this AchillesTestResource will use an unsafe Cassandra daemon, it is not suitable for production</strong>
 * <br/><br/>
 * Test resource for JUnit. Example of usage:
 *
 * <pre class="code"><code class="java">
 *
 * {@literal @}Rule
 * public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
 * .forJunit()
 * .withKeyspace("unit_test") // default keyspace = achilles_embedded
 * .entityClassesToTruncate(SimpleEntity.class)
 * .truncateBeforeAndAfterTest()
 * .build((cluster, statementsCache) -> ManagerFactoryBuilder
 * .builder(cluster)
 * .doForceSchemaCreation(true)
 * .withStatementCache(statementsCache)
 * .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
 * .build()
 * );


 * private Session session = resource.getNativeSession();

 * private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

 * private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();

 * {@literal @}Test
 * public void should_test_xxx() throws Exception {

 * //Given
 * final long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
 * scriptExecutor.executeScriptTemplate("SimpleEntity/insert_single_row.cql", ImmutableMap.of("id", id));

 * //When

 * //Then
 * Row actual = session.execute("SELECT ....").one();

 * assertTrue(row.getString("xxx").equals("yyy"));
 * ...
 * }
 * </code></pre>
 */
public class AchillesTestResource<T extends AbstractManagerFactory> extends ExternalResource {


    // Default statement cache for unit testing
    private static final StatementsCache STATEMENTS_CACHE = new StatementsCache(10000);
    private static final Logger DML_LOG = LoggerFactory.getLogger(AchillesLoggers.ACHILLES_DML_STATEMENT);
    private static final Map<String, PreparedStatement> TABLES_TO_TRUNCATE = new ConcurrentHashMap<>();

    private final TypedMap cassandraParams;
    private final Optional<String> keyspaceName;
    private final List<PreparedStatement> truncateStatements;
    private final CassandraEmbeddedServer server;
    private final T managerFactory;
    private final Session session;
    private final ScriptExecutor scriptExecutor;
    private final Steps steps;

    public AchillesTestResource(BiFunction<Cluster, StatementsCache, T> managerFactoryBuilder, TypedMap cassandraParams,
                                Optional<String> keyspaceName, List<String> tablesToTruncate, List<Class<?>> entityClassesToTruncate) {
        this(managerFactoryBuilder, cassandraParams, keyspaceName, BOTH, tablesToTruncate, entityClassesToTruncate);
    }

    public AchillesTestResource(BiFunction<Cluster, StatementsCache, T> managerFactoryBuilder, TypedMap cassandraParams,
                                Optional<String> keyspaceName, Steps cleanUpSteps, List<String> tablesToTruncate, List<Class<?>> entityClassesToTruncate) {
        this.cassandraParams = cassandraParams;
        this.keyspaceName = keyspaceName;
        this.steps = cleanUpSteps;
        this.server = buildServer();
        this.session = buildSession(this.server);
        this.scriptExecutor = new ScriptExecutor(this.session);
        this.managerFactory = buildManagerFactory(this.server, managerFactoryBuilder);
        this.truncateStatements = determineTableToTruncate(this.managerFactory, this.session, tablesToTruncate, entityClassesToTruncate);
    }

    public Session getNativeSession() {
        return this.session;
    }

    public ScriptExecutor getScriptExecutor() {
        return this.scriptExecutor;
    }

    public T getManagerFactory() {
        return this.managerFactory;
    }

    private CassandraEmbeddedServer buildServer() {
        return CassandraEmbeddedServerBuilder
                .builder()
                .withParams(cassandraParams)
                .buildServer();
    }

    private T buildManagerFactory(CassandraEmbeddedServer server, BiFunction<Cluster, StatementsCache, T> managerFactoryBuilder) {
        return managerFactoryBuilder.apply(server.getNativeCluster(), STATEMENTS_CACHE);
    }

    private Session buildSession(CassandraEmbeddedServer server) {
        final Session defaultSession = server.getNativeSession();

        final Session session = keyspaceName
                .filter(ks -> !ks.equals(defaultSession.getLoggedKeyspace()))
                .map(x -> defaultSession.getCluster().connect(x))
                .orElse(defaultSession);

        server.registerSessionForShutdown(session);

        return session;
    }

    private List<PreparedStatement> determineTableToTruncate(T managerFactory, Session session, List<String> tablesToTruncate, List<Class<?>> entityClassesToTruncate) {

        entityClassesToTruncate
                .forEach(clazz -> validateTrue(managerFactory.staticTableNameFor(clazz).isPresent(),
                        "Entity class '%s' is not managed by Achilles. Did you forget to add @Table annotation ?", clazz.getCanonicalName()));

        maybeGenerateTruncateStatement(session, entityClassesToTruncate
                .stream()
                .map(clazz -> managerFactory.staticTableNameFor(clazz).get())
                .collect(toList()));

        maybeGenerateTruncateStatement(session, tablesToTruncate);

        return
                Stream.concat(tablesToTruncate.stream(),
                        entityClassesToTruncate.stream().map(clazz -> managerFactory.staticTableNameFor(clazz).get()))
                        .map(TABLES_TO_TRUNCATE::get)
                        .collect(toList());
    }

    private void maybeGenerateTruncateStatement(Session session, List<String> tablesToTruncate) {
        tablesToTruncate
                .stream()
                .filter(tableName -> !TABLES_TO_TRUNCATE.containsKey(tableName))
                .forEach(table -> TABLES_TO_TRUNCATE.put(table, session.prepare("TRUNCATE " + table)));
    }


    protected void before() throws Throwable {
        if (steps.isBefore())
            truncateTables();
    }

    protected void after() {
        if (steps.isAfter())
            truncateTables();
    }

    public void truncateTables() {
        truncateStatements
                .forEach(statement -> {
                    if (DML_LOG.isDebugEnabled()) {
                        DML_LOG.debug(statement.getQueryString());
                    }
                    session.execute(statement.bind());
                });
    }

    public enum Steps {
        BEFORE_TEST, AFTER_TEST, BOTH;

        public boolean isBefore() {
            return (this == BOTH || this == BEFORE_TEST);
        }

        public boolean isAfter() {
            return (this == BOTH || this == AFTER_TEST);
        }
    }
}
