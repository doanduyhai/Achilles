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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Duration;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactoryBuilder_For_IT_3_10;
import info.archinnov.achilles.generated.ManagerFactory_For_IT_3_10;
import info.archinnov.achilles.generated.manager.EntityWithDurationType_Manager;
import info.archinnov.achilles.internals.entities.EntityWithDurationType;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public class TestEntityWithDurationTypeIT {

    @Rule
    public AchillesTestResource<ManagerFactory_For_IT_3_10> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("it_3_10")
            .entityClassesToTruncate(EntityWithDurationType.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder_For_IT_3_10
                    .builder(cluster)
                    .withDefaultKeyspaceName("it_3_10")
                    .withManagedEntityClasses(EntityWithDurationType.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithDurationType_Manager manager = resource.getManagerFactory().forEntityWithDurationType();

    @Test
    public void should_insert() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Duration duration = Duration.from("10h30m10s");
        final EntityWithDurationType entity = new EntityWithDurationType(id, duration);

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        final Row found = session.execute("SELECT * FROM it_3_10.entity_with_duration_type WHERE id = " + id).one();

        assertThat(found).isNotNull();
        assertThat(found.get("duration", Duration.class)).isEqualTo(duration);
    }

    @Test
    public void should_find_by_id() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithDurationType/insert_single_row.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithDurationType found = manager
                .crud()
                .findById(id)
                .get();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.duration).isEqualTo(Duration.from("10h30m10s"));
    }

    @Test
    public void should_dsl_select() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        scriptExecutor.executeScriptTemplate("EntityWithDurationType/insert_single_row.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithDurationType found = manager
                .dsl()
                .select()
                .duration()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getOne();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.duration).isEqualTo(Duration.from("10h30m10s"));
    }
}
