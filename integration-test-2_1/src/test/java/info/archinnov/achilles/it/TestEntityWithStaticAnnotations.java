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

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithStaticAnnotations_Manager;
import info.archinnov.achilles.internals.entities.EntityWithStaticAnnotations;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

public class TestEntityWithStaticAnnotations {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestEntityWithStaticAnnotations.class);

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .createAndUseKeyspace("my_static_keyspace")
            .entityClassesToTruncate(EntityWithStaticAnnotations.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithStaticAnnotations.class)
                    .withDefaultKeyspaceName("my_static_keyspace")
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .build());

    private Session session = resource.getNativeSession();
    private EntityWithStaticAnnotations_Manager manager = resource.getManagerFactory().forEntityWithStaticAnnotations();

    @Test
    public void should_insert_using_static_insert_strategy_and_consistency_level() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticAnnotations entity = new EntityWithStaticAnnotations(id, "val", "overriden_val");

        manager
                .crud()
                .insert(entity)
                .usingTimeToLive(1000)
                .execute();

        final EntityWithStaticAnnotations newEntity = new EntityWithStaticAnnotations(id, "new_val", null);
        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        manager
                .crud()
                .insert(newEntity)
                .usingTimeToLive(1000)
                .execute();

        //Then
        Row actual = session.execute("SELECT * FROM entity_static_annotations WHERE partition_key = " + id).one();

        assertThat(actual).isNotNull();
        assertThat(actual.getString("value")).isEqualTo("new_val");
        /*
         *  Since the InsertStrategy is NOT NULL FIELDS, the value of "overRiden" is "overriden_val" as
         *  it was inserted previously. The new "null" value is not taken into account.
         */
        assertThat(actual.getString("\"overRiden\"")).isEqualTo("overriden_val");

        logAsserter.assertConsistencyLevels(LOCAL_ONE);
    }

    @Test
    public void should_insert_using_static_ttl() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticAnnotations entity = new EntityWithStaticAnnotations(id, "new_val", "overriden_val");

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then

        LOGGER.warn("Waiting for 1 sec to allow TTL data to expire ");

        Thread.sleep(1000);

        Row actual = session.execute("SELECT * FROM entity_static_annotations WHERE partition_key = " + id).one();
        assertThat(actual).isNull();
    }

    @Test
    public void should_insert_overriding_static_conf() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticAnnotations entity = new EntityWithStaticAnnotations(id, "new_val", "overriden_val");

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        manager
                .crud()
                .insert(entity)
                .usingTimeToLive(10)
                .withConsistencyLevel(LOCAL_QUORUM)
                .execute();


        //Then
        LOGGER.warn("Waiting for 1 sec to allow TTL data to expire ");

        Thread.sleep(1000);

        Row actual = session.execute("SELECT * FROM my_static_keyspace.entity_static_annotations WHERE partition_key = " + id)
                .one();
        assertThat(actual).isNotNull();
        assertThat(actual.getString("\"overRiden\"")).isEqualTo("overriden_val");

        logAsserter.assertConsistencyLevels(LOCAL_QUORUM);
    }

    @Test
    public void should_find_using_static_consistency() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticAnnotations entity = new EntityWithStaticAnnotations(id, "new_val", "overriden_val");
        manager
                .crud()
                .insert(entity)
                .usingTimeToLive(1000)
                .execute();

        final CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();


        //When
        final EntityWithStaticAnnotations actual = manager
                .crud()
                .findById(id)
                .get();

        //Then
        assertThat(actual).isNotNull();

        logAsserter.assertConsistencyLevels(LOCAL_QUORUM);
    }
}
