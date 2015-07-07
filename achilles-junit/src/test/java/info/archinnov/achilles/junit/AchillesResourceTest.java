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
package info.archinnov.achilles.junit;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.SCRIPT_LOCATIONS;
import static info.archinnov.achilles.internal.utils.ConfigMap.fromMap;
import static org.fest.assertions.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.configuration.ConfigurationParameters;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.TypedMap;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;
import info.archinnov.achilles.test.integration.entity.User;

import java.util.ArrayList;

public class AchillesResourceTest {

    private static final String CUSTOM_KEYSPACE_NAME = "my_keyspace";

    private TypedMap parameters = TypedMap.fromMap(ImmutableMap.<String, Object>of(SCRIPT_LOCATIONS, new ArrayList<String>()));

    @Rule
    public AchillesResource resource = new AchillesResource(parameters, fromMap(ImmutableMap.<ConfigurationParameters, Object>of(KEYSPACE_NAME, CUSTOM_KEYSPACE_NAME, ENTITY_PACKAGES, "info.archinnov.achilles.test.integration.entity")),
            Steps.AFTER_TEST, "User");

    private PersistenceManagerFactory pmf = resource.getPersistenceManagerFactory();
    private PersistenceManager manager = resource.getPersistenceManager();
    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();


    @Test
    public void should_bootstrap_embedded_server_and_entity_manager() throws Exception {

        Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
        manager.insert(new User(id, "fn", "ln"));

        Row row = session.execute("SELECT * FROM User WHERE id=" + id).one();

        assertThat(row).isNotNull();

        assertThat(row.getString("firstname")).isEqualTo("fn");
        assertThat(row.getString("lastname")).isEqualTo("ln");
    }

    @Test
    public void should_create_resources_once() throws Exception {
        AchillesResource resource = new AchillesResource(new TypedMap(), fromMap(ImmutableMap.<ConfigurationParameters, Object>of(KEYSPACE_NAME, CUSTOM_KEYSPACE_NAME, ENTITY_PACKAGES, "info.archinnov.achilles.junit.test.entity")));

        assertThat(resource.getPersistenceManagerFactory()).isSameAs(pmf);
        assertThat(resource.getPersistenceManager()).isSameAs(manager);
        assertThat(resource.getNativeSession()).isSameAs(session);
    }

    @Test
    public void should_execute_script_using_the_executor() throws Exception {
        //Given
        scriptExecutor.executeScript("script_on_the_fly.cql");

        //When
        final Row row = session.execute("SELECT * FROM user WHERE id=123").one();

        //Then
        assertThat(row.getString("firstname")).isEqualTo("Albert");
        assertThat(row.getString("lastname")).isEqualTo("EINSTEIN");
    }
}
