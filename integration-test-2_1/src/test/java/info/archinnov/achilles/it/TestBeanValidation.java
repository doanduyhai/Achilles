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

package info.archinnov.achilles.it;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static info.archinnov.achilles.type.interceptor.Event.PRE_INSERT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithBeanValidation_Manager;
import info.archinnov.achilles.internals.entities.EntityWithBeanValidation;
import info.archinnov.achilles.internals.entities.TestUDT;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.interceptor.Event;
import info.archinnov.achilles.type.interceptor.Interceptor;

public class TestBeanValidation {

    private static Interceptor<EntityWithBeanValidation> PRE_INSERT_INTERCEPTOR = new Interceptor<EntityWithBeanValidation>() {
        @Override
        public boolean acceptEntity(Class<?> entityClass) {
            return entityClass.equals(EntityWithBeanValidation.class);
        }

        @Override
        public void onEvent(EntityWithBeanValidation entity, Event event) {
            if (MapUtils.isEmpty(entity.getUdt().getMap())) {
                entity.getUdt().setMap(ImmutableMap.of(0, "default"));
            }
        }

        @Override
        public List<Event> interceptOnEvents() {
            return asList(PRE_INSERT);
        }
    };

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithBeanValidation.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithBeanValidation.class)
                    .doForceSchemaCreation(true)
                    .withBeanValidation(true)
                    .withPostLoadBeanValidation(true)
                    .withEventInterceptors(asList(PRE_INSERT_INTERCEPTOR))
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .withStatementsCache(statementsCache)
                    .build());

    private Session session = resource.getNativeSession();
    private EntityWithBeanValidation_Manager manager = resource.getManagerFactory().forEntityWithBeanValidation();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();

    @Test
    public void should_fail_on_pre_insert_because_empty_text() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final TestUDT udt = new TestUDT("test@test.com", asList("list"), ImmutableMap.of(1, "one"));
        final EntityWithBeanValidation entity = new EntityWithBeanValidation(id, "", asList("1", "2"), udt);

        //When
        exception.expect(AchillesBeanValidationException.class);
        exception.expectMessage("Bean validation error on event 'PRE_INSERT' : \n" +
                "\tproperty 'value' of class 'info.archinnov.achilles.internals.entities.EntityWithBeanValidation' should not be blank");

        manager
                .crud()
                .insert(entity)
                .execute();
    }

    @Test
    public void should_fail_on_pre_insert_because_empty_list() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final TestUDT udt = new TestUDT("test@test.com", asList("list"), ImmutableMap.of(1, "one"));
        final EntityWithBeanValidation entity = new EntityWithBeanValidation(id, "value", new ArrayList<>(), udt);

        //When
        exception.expect(AchillesBeanValidationException.class);
        exception.expectMessage("Bean validation error on event 'PRE_INSERT' : \n" +
                "\tproperty 'list' of class 'info.archinnov.achilles.internals.entities.EntityWithBeanValidation' should not be empty");

        manager
                .crud()
                .insert(entity)
                .execute();
    }

    @Test
    public void should_fail_on_pre_insert_because_invalid_email_in_udt() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final TestUDT udt = new TestUDT("test", asList("list"), ImmutableMap.of(1, "one"));
        final EntityWithBeanValidation entity = new EntityWithBeanValidation(id, "value", asList(""), udt);

        //When
        exception.expect(AchillesBeanValidationException.class);
        exception.expectMessage("Bean validation error on event 'PRE_INSERT' : \n" +
                "\tproperty 'udt.name' of class 'info.archinnov.achilles.internals.entities.TestUDT' not a well-formed email address");

        manager
                .crud()
                .insert(entity)
                .execute();
    }

    @Test
    public void should_fail_on_pre_insert_because_failed_constraint_in_udt() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final TestUDT udt = new TestUDT("test@test.com", null, ImmutableMap.of(1, "one"));
        final EntityWithBeanValidation entity = new EntityWithBeanValidation(id, "value", asList(""), udt);

        //When
        exception.expect(AchillesBeanValidationException.class);
        exception.expectMessage("Bean validation error on event 'PRE_INSERT' : \n" +
                "\tproperty 'udt.list' of class 'info.archinnov.achilles.internals.entities.TestUDT' UDT list should not be empty");

        manager
                .crud()
                .insert(entity)
                .execute();
    }

    @Test
    public void should_verify_bean_validator_called_last_on_pre_insert() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final TestUDT udt = new TestUDT("test@test.com", asList("1"), null);
        final EntityWithBeanValidation entity = new EntityWithBeanValidation(id, "value", asList(""), udt);

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        Row actual = session.execute("SELECT * FROM bean_validation WHERE id = " + id).one();
        assertThat(actual).isNotNull();
        assertThat(actual.getUDTValue("udt").getMap("map", String.class, String.class))
                .containsEntry("0", "default");
    }

    @Test
    public void should_fail_on_post_load() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithBeanValidation/insert_single_row.cql", ImmutableMap.of("id", id));

        exception.expect(AchillesException.class);
        exception.expectMessage("Bean validation error on event 'POST_LOAD' : \n" +
                "\tproperty 'list' of class 'info.archinnov.achilles.internals.entities.EntityWithBeanValidation' should not be empty");

        manager
                .crud()
                .findById(id)
                .get();
    }
}
