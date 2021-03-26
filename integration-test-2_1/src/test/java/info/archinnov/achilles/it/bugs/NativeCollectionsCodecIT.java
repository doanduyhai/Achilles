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

package info.archinnov.achilles.it.bugs;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithNativeCollections_Manager;
import info.archinnov.achilles.internals.entities.EntityWithNativeCollections;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.tuples.Tuple2;

public class NativeCollectionsCodecIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithNativeCollections.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
            .build((cluster, statementsCache) ->
                ManagerFactoryBuilder
                        .builder(cluster)
                        .withManagedEntityClasses(EntityWithNativeCollections.class)
                        .doForceSchemaCreation(true)
                        .withStatementsCache(statementsCache)
                        .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                        .build()
            );

    private EntityWithNativeCollections_Manager manager = resource.getManagerFactory().forEntityWithNativeCollections();

    @Test
    public void should_insert_list_long() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final List<Long> longs = Arrays.asList(1L, 2L);
        final List<Double> doubles = Arrays.asList(1.0D, 2.0D);
        final Map<Integer, Long> mapIntLong = ImmutableMap.of(1, 1L, 2, 2L);
        final Tuple2<List<Integer>, List<Double>> tuple2 = Tuple2.of(Arrays.asList(1, 2), Arrays.asList(1.0, 2.0));
        final EntityWithNativeCollections entity = new EntityWithNativeCollections(id, longs, doubles, mapIntLong, tuple2);

        //When
        manager.crud()
                .insert(entity)
                .withInsertStrategy(InsertStrategy.NOT_NULL_FIELDS)
                .execute();

        //Then
        final Row found = manager.getNativeSession().execute("SELECT * FROM achilles_embedded.entity_with_native_collections WHERE id = " + id).one();
        assertThat(found).isNotNull();
    }


}
