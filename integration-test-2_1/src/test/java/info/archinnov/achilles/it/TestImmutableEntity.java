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
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.ImmutableEntity_Manager;
import info.archinnov.achilles.internals.entities.ImmutableEntity;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestImmutableEntity {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(ImmutableEntity.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(ImmutableEntity.class)
                .doForceSchemaCreation(true)
                .withStatementsCache(statementsCache)
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
            .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private ImmutableEntity_Manager manager = resource.getManagerFactory().forImmutableEntity();

    @Test
    public void should_find_by_id() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("ImmutableEntity/should_insert_row.cql", ImmutableMap.of("id", id));

        //When
        final ImmutableEntity actual = manager.crud().findById(id).get();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.name).isEqualTo("name");
        assertThat(actual.value).isEqualTo(2.0);
        assertThat(actual.udt).isNotNull();
        assertThat(actual.udt.idx).isEqualTo(3L);
        assertThat(actual.udt.value).isEqualTo("value_udt");
    }

    @Test
    public void should_dsl_select_one() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("ImmutableEntity/should_insert_row.cql", ImmutableMap.of("id", id));

        //When
        final ImmutableEntity actual = manager.dsl().select().allColumns_FromBaseTable().where().id().Eq(id).getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.name).isEqualTo("name");
        assertThat(actual.value).isEqualTo(2.0);
        assertThat(actual.udt).isNotNull();
        assertThat(actual.udt.idx).isEqualTo(3L);
        assertThat(actual.udt.value).isEqualTo("value_udt");
    }

    @Test
    public void should_typed_query() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("ImmutableEntity/should_insert_row.cql", ImmutableMap.of("id", id));

        //When
        final SimpleStatement statement = new SimpleStatement("SELECT id,value,udt FROM achilles_embedded.immutable_entity WHERE id=?");
        final PreparedStatement ps = session.prepare(statement);
        final ImmutableEntity actual = manager.raw().typedQueryForSelect(ps.bind(id)).getOne();

        //Then
        assertThat(actual).isNotNull();
        assertThat(actual.name).isNull();
        assertThat(actual.value).isEqualTo(2.0);
        assertThat(actual.udt).isNotNull();
        assertThat(actual.udt.idx).isEqualTo(3L);
        assertThat(actual.udt.value).isEqualTo("value_udt");
    }
}
