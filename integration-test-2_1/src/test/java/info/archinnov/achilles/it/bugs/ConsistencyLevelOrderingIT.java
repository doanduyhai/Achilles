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

import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityAsChild_Manager;
import info.archinnov.achilles.generated.manager.EntityWithStaticAnnotations_Manager;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.EntityAsChild;
import info.archinnov.achilles.internals.entities.EntityWithStaticAnnotations;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

public class ConsistencyLevelOrderingIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class, EntityAsChild.class, EntityWithStaticAnnotations.class)
            .truncateBeforeAndAfterTest()
            .withScript("create_keyspace.cql")
            .build((cluster, statementsCache) -> {

                // Consistency level set on Cluster object
                cluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.ALL);

                return ManagerFactoryBuilder
                        .builder(cluster)
                        .withManagedEntityClasses(SimpleEntity.class, EntityAsChild.class, EntityWithStaticAnnotations.class)
                        .doForceSchemaCreation(true)
                        .withStatementsCache(statementsCache)
                        .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                        .withDefaultWriteConsistency(ConsistencyLevel.LOCAL_QUORUM)
                        .withDefaultWriteConsistencyMap(ImmutableMap.of(
                                "simple", ConsistencyLevel.LOCAL_ONE,
                                "entity_static_annotations", ConsistencyLevel.TWO))
                        .build();
            });

    private SimpleEntity_Manager simpleEntityManager = resource.getManagerFactory().forSimpleEntity();
    private EntityWithStaticAnnotations_Manager entityWithStaticAnnotationsManager = resource.getManagerFactory().forEntityWithStaticAnnotations();
    private EntityAsChild_Manager entityAsChildManager = resource.getManagerFactory().forEntityAsChild();
    
    @Test
    public void should_override_cluster_consistency_level_by_consistency_level_map_value() throws Exception {
        //Given
        Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);

        CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        simpleEntityManager
                .crud()
                .insert(new SimpleEntity(id, new Date(), "value"))
                .execute();


        //Then
        logAsserter.assertConsistencyLevels(ConsistencyLevel.LOCAL_ONE);
    }

    @Test
    public void should_override_consistency_level_map_value_by_static_consistency_setting() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticAnnotations entity = new EntityWithStaticAnnotations(id, "new_val", "overriden_val");

        CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        entityWithStaticAnnotationsManager
                .crud()
                .insert(entity)
                .execute();

        //Then
        logAsserter.assertConsistencyLevels(ConsistencyLevel.LOCAL_ONE);
    }

    @Test
    public void should_override_static_consistency_setting_by_runtime_value() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityWithStaticAnnotations entity = new EntityWithStaticAnnotations(id, "new_val", "overriden_val");

        CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        entityWithStaticAnnotationsManager
                .crud()
                .insert(entity)
                .withConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
                .execute();

        //Then
        logAsserter.assertConsistencyLevels(ConsistencyLevel.EACH_QUORUM);
    }

    @Test
    public void should_override_cluster_consistency_config_by_achilles_consistency_setting() throws Exception {
        //Given
        final long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final EntityAsChild entityAsChild = new EntityAsChild(id, "val", "another_val");

        CassandraLogAsserter logAsserter = new CassandraLogAsserter();
        logAsserter.prepareLogLevelForDriverConnection();

        //When
        entityAsChildManager
                .crud()
                .insert(entityAsChild)
                .execute();

        //Then
        logAsserter.assertConsistencyLevels(ConsistencyLevel.LOCAL_QUORUM);
    }
}
