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

package info.archinnov.achilles.test.integration.tests;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.assertThat;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.persistence.PersistenceManagerFactory.PersistenceManagerFactoryBuilder;

public class EntityLessIT {

    private static final String TEST_KEYSPACE = "test_keyspace";

    private Session session = CassandraEmbeddedServerBuilder.noEntityPackages().withKeyspaceName(TEST_KEYSPACE)
            .cleanDataFilesAtStartup(true).buildNativeSessionOnly();

    private PersistenceManager manager;

    @Before
    public void setUp() {
        PersistenceManagerFactory factory = PersistenceManagerFactoryBuilder.builder(session.getCluster())
                .withNativeSession(session).withKeyspaceName(TEST_KEYSPACE).build();
        manager = factory.createPersistenceManager();
    }

    @Test
    public void should_bootstrap_achilles_without_entity_package_for_native_query() throws Exception {
        RegularStatement statement = select("keyspace_name").from("system","schema_keyspaces").where(eq("keyspace_name","system"));

        Map<String, Object> keyspaceMap = manager.nativeQuery(statement).getFirst();

        assertThat(keyspaceMap).hasSize(1);
        assertThat(keyspaceMap.get("keyspace_name")).isEqualTo("system");
    }

}
