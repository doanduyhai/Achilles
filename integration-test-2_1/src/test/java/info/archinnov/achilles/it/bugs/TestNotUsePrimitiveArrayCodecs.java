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

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Sets;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithUDTCollectionsPrimitive_Manager;
import info.archinnov.achilles.internals.entities.EntityWithUDTCollectionsPrimitive;
import info.archinnov.achilles.internals.entities.UDTWithCollectionsPrimitive;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

public class TestNotUsePrimitiveArrayCodecs {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithUDTCollectionsPrimitive.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithUDTCollectionsPrimitive.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    final private Session session = resource.getNativeSession();
    final private EntityWithUDTCollectionsPrimitive_Manager manager = resource.getManagerFactory().forEntityWithUDTCollectionsPrimitive();

    @Test
    public void should_insert_udt_with_collections_primitive() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0L, Long.MAX_VALUE);
        final UDTWithCollectionsPrimitive udt = new UDTWithCollectionsPrimitive(Arrays.asList(1, 2, 3), Sets.newHashSet(4.0d, 5.0d, 6.0d));
        final EntityWithUDTCollectionsPrimitive entity = new EntityWithUDTCollectionsPrimitive(id, udt);

        //When
        manager
                .crud()
                .insert(entity)
                .execute();

        //Then
        final Row found = session.execute("SELECT * FROM achilles_embedded.entity_with_udt_collections_primitives WHERE id = " + id).one();
        assertThat(found).isNotNull();
        final UDTValue udtValue = found.getUDTValue("udt");
        assertThat(udtValue).isNotNull();
        assertThat(udtValue.getList("listint", Integer.class)).containsExactly(1, 2, 3);
        assertThat(udtValue.getSet("setdouble", Double.class)).contains(4.0d, 5.0d, 6.0d);
    }
}
