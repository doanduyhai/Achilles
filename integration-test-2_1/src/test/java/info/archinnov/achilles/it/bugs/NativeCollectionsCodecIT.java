/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

package info.archinnov.achilles.it.bugs;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;


import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;


import com.datastax.driver.core.Row;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithListLong_Manager;
import info.archinnov.achilles.internals.entities.EntityWithListLong;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

@RunWith(MockitoJUnitRunner.class)
public class NativeCollectionsCodecIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithListLong.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
            .build((cluster, statementsCache) ->
                ManagerFactoryBuilder
                        .builder(cluster)
                        .withManagedEntityClasses(EntityWithListLong.class)
                        .doForceSchemaCreation(true)
                        .withStatementsCache(statementsCache)
                        .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                        .build()
            );

    private EntityWithListLong_Manager manager = resource.getManagerFactory().forEntityWithListLong();

    @Test
    public void should_insert_list_long() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final List<Long> longs = Arrays.asList(1L, 2L);
        final EntityWithListLong entity = new EntityWithListLong(id, longs);

        //When
        manager.crud()
                .insert(entity)
//                .withInsertStrategy(InsertStrategy.NOT_NULL_FIELDS)
                .execute();

        //Then
        final Row found = manager.getNativeSession().execute("SELECT * FROM achilles_embedded.entity_with_list_long WHERE id = " + id).one();
        assertThat(found).isNotNull();
    }


}
