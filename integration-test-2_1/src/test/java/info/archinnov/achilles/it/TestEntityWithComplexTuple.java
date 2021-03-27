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
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomUtils;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithComplexTuple_Manager;
import info.archinnov.achilles.internals.entities.EntityWithComplexTuple;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.script.ScriptExecutor;
import info.archinnov.achilles.type.tuples.Tuple2;

public class TestEntityWithComplexTuple {

    @ClassRule
    public static AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithComplexTuple.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithComplexTuple.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private Session session = resource.getNativeSession();
    private ScriptExecutor scriptExecutor = resource.getScriptExecutor();
    private EntityWithComplexTuple_Manager manager = resource.getManagerFactory().forEntityWithComplexTuple();

    @AfterClass
    public static void cleanUp() {
        resource.getManagerFactory().shutDown();
    }

    @Test
    public void should_insert() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final Tuple2<Integer, Map<Integer, List<String>>> tuple = Tuple2.of(10, ImmutableMap.of(20, asList("10", "20")));

        final EntityWithComplexTuple entity = new EntityWithComplexTuple(id, tuple);

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        final Row actual = session.execute("SELECT * FROM complex_tuple WHERE id = " + id).one();

        assertThat(actual).isNotNull();
        final TupleValue tupleValue = actual.getTupleValue("tuple");
        assertThat(tupleValue.getInt(0)).isEqualTo(10);
        final Map<Integer, List<String>> map = tupleValue.getMap(1, new TypeToken<Integer>() {
        }, new TypeToken<List<String>>() {
        });
        assertThat(map).containsEntry(20, asList("10", "20"));

    }

    @Test
    public void should_find() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        scriptExecutor.executeScriptTemplate("EntityWithComplexTuple/insert_single_row.cql", ImmutableMap.of("id", id));

        //When
        final EntityWithComplexTuple actual = manager
                .crud()
                .findById(id)
                .get();

        //Then
        assertThat(actual).isNotNull();
        final Tuple2<Integer, Map<Integer, List<String>>> tuple = actual.getTuple();
        assertThat(tuple._1()).isEqualTo(10);
        assertThat(tuple._2()).containsEntry(20, asList("10", "20"));
    }
}
