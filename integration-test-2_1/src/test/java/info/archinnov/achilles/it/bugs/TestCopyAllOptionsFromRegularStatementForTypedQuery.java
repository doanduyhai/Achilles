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

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.SimpleEntity;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

public class TestCopyAllOptionsFromRegularStatementForTypedQuery {


    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(SimpleEntity.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(SimpleEntity.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private SimpleEntity_Manager manager = resource.getManagerFactory().forSimpleEntity();


    @Test
    public void should_use_type_query_with_fetch_size() throws Exception {
        //Given
        final Long id1 = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final Long id2 = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final Long id3 = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final Date date = new Date();
        final SimpleEntity entity1 = new SimpleEntity(id1, date, "val");
        final SimpleEntity entity2 = new SimpleEntity(id2, date, "val");
        final SimpleEntity entity3 = new SimpleEntity(id3, date, "val");

        manager.crud().insert(entity1).execute();
        manager.crud().insert(entity2).execute();
        manager.crud().insert(entity3).execute();


        //When
        final RegularStatement regularStatement = QueryBuilder.select().all().from(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME, "simple");
        regularStatement.setFetchSize(1);

        final List<SimpleEntity> found = manager.raw().typedQueryForSelect(regularStatement).getList();

        //Then
        assertThat(found).hasSize(1);
    }
}
