/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithNestedUDT_Manager;
import info.archinnov.achilles.internals.entities.EntityWithNestedUDT;
import info.archinnov.achilles.internals.entities.UDTWithNestedUDT;
import info.archinnov.achilles.internals.entities.UDTWithNoKeyspace;
import info.archinnov.achilles.internals.query.crud.FindWithOptions;
import info.archinnov.achilles.type.tuples.Tuple2;

@RunWith(MockitoJUnitRunner.class)
public class TestEntityWithNestedUdtIT {

    @Test
    public void should_insert_nested_udt() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder
                .builder()
                .useUnsafeCassandraDeamon()
                .withScript("functions/createFunctions.cql")
                .buildNativeCluster();

        final ManagerFactory managerFactory = ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithNestedUDT.class)
                .doForceSchemaCreation(true)
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                .build();

        final EntityWithNestedUDT_Manager manager = managerFactory.forEntityWithNestedUDT();

        //When
        final Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final EntityWithNestedUDT entity = new EntityWithNestedUDT();
        final UDTWithNoKeyspace udtWithNoKeySpace = new UDTWithNoKeyspace();
        udtWithNoKeySpace.setId(id);
        udtWithNoKeySpace.setValue("udt_with_no_keyspace");
        final UDTWithNestedUDT udtWithNestedUDT = new UDTWithNestedUDT();
        udtWithNestedUDT.setValue("value");
        udtWithNestedUDT.setNestedUDT(udtWithNoKeySpace);
        udtWithNestedUDT.setUdtList(Arrays.asList(udtWithNoKeySpace));
        udtWithNestedUDT.setTupleWithUDT(new Tuple2<>(1, udtWithNoKeySpace));
        entity.setId(id);
        entity.setUdt(udtWithNoKeySpace);
        entity.setComplexUDT(udtWithNestedUDT);

        manager.crud().insert(entity).execute();

        //Then
        final EntityWithNestedUDT found = manager.crud().findById(id).get();
        assertThat(found).isNotNull();
        assertThat(found.getUdt()).isEqualTo(udtWithNoKeySpace);
        assertThat(found.getComplexUDT()).isEqualTo(udtWithNestedUDT);
    }
}
