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
import java.util.Optional;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithNestedUDT_Manager;
import info.archinnov.achilles.internals.entities.EntityWithNestedUDT;
import info.archinnov.achilles.internals.entities.UDTWithNestedUDT;
import info.archinnov.achilles.internals.entities.UDTWithNoKeyspace;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;

public class TestEntityWithNestedUdtIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityWithNestedUDT.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityWithNestedUDT.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
            .build());


    private EntityWithNestedUDT_Manager manager = resource.getManagerFactory().forEntityWithNestedUDT();

    @Test
    public void should_insert_nested_udt() throws Exception {
        //Given
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
        entity.setOptionalUDT(Optional.of(udtWithNoKeySpace));

        //When
        manager.crud().insert(entity).execute();

        //Then
        final EntityWithNestedUDT found = manager.crud().findById(id).get();
        assertThat(found).isNotNull();
        assertThat(found.getUdt()).isEqualTo(udtWithNoKeySpace);
        assertThat(found.getComplexUDT()).isEqualTo(udtWithNestedUDT);
        assertThat(found.getOptionalUDT().isPresent()).isTrue();
        assertThat(found.getOptionalUDT().get()).isEqualTo(udtWithNoKeySpace);
    }

    @Test
    public void should_update_nested_udt() throws Exception {
        //Given
        final Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final EntityWithNestedUDT entity = new EntityWithNestedUDT();
        final UDTWithNoKeyspace udtWithNoKeySpace = new UDTWithNoKeyspace();
        udtWithNoKeySpace.setId(id);
        udtWithNoKeySpace.setValue("udt_with_no_keyspace");

        entity.setId(id);
        entity.setUdt(udtWithNoKeySpace);

        manager.crud().insert(entity);

        //When
        udtWithNoKeySpace.setValue("new_udt_value");

        manager
                .dsl()
                .update()
                .fromBaseTable()
                .udt().Set(udtWithNoKeySpace)
                .where()
                .id().Eq(id)
                .execute();

        //Then
        final EntityWithNestedUDT found = manager.crud().findById(id).get();
        assertThat(found.getUdt().getValue()).isEqualTo("new_udt_value");
    }

    @Test
    public void should_select_some_udt_columns() throws Exception {
        //Given
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

        //When
        final TypedMap found = manager.dsl()
                .select()
                .complexUDT().value()
                .complexUDT().nestedUDT().value()
                .udt().value()
                .fromBaseTable()
                .where()
                .id().Eq(id)
                .getTypedMap();


        //Then
        assertThat(found).isNotNull();
        assertThat(found.<String>getTyped("complexudt.value")).isEqualTo("value");
        assertThat(found.<String>getTyped("complexudt.nestedudt.VALUE")).isEqualTo("udt_with_no_keyspace");
        assertThat(found.<String>getTyped("udt.VALUE")).isEqualTo("udt_with_no_keyspace");
    }

}
