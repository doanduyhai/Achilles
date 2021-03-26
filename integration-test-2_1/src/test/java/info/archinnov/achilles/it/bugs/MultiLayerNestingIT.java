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

import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.UDTValue;

import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityLayer1_Manager;
import info.archinnov.achilles.internals.entities.EntityLayer1;
import info.archinnov.achilles.internals.entities.Layer2;
import info.archinnov.achilles.internals.entities.Layer3;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;
import info.archinnov.achilles.type.TypedMap;

public class MultiLayerNestingIT {

    @Rule
    public AchillesTestResource<ManagerFactory> resource = AchillesTestResourceBuilder
            .forJunit()
            .entityClassesToTruncate(EntityLayer1.class)
            .truncateBeforeAndAfterTest()
            .build((cluster, statementsCache) -> ManagerFactoryBuilder
                    .builder(cluster)
                    .withManagedEntityClasses(EntityLayer1.class)
                    .doForceSchemaCreation(true)
                    .withStatementsCache(statementsCache)
                    .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                    .build());

    private  final EntityLayer1_Manager manager = resource.getManagerFactory().forEntityLayer1();

    @Test
    public void should_handle_3_levels_of_nesting_udt() throws Exception {
       //Given
       final EntityLayer1 entity = new EntityLayer1("layer1", new Layer2("layer2", new Layer3("layer3")));
       manager.crud().insert(entity).execute();

       //When
       final EntityLayer1 found = manager.crud().findById("layer1").get();

       //Then
       assertThat(found).isNotNull();
       assertThat(found.getLayer()).isEqualTo("layer1");
       assertThat(found.getLayer2()).isNotNull();
       assertThat(found.getLayer2().getLayer()).isEqualTo("layer2");
       assertThat(found.getLayer2().getLayer3()).isNotNull();
       assertThat(found.getLayer2().getLayer3().getLayer()).isEqualTo("layer3");
   }
    
    @Test
    public void should_select_some_columns_from_udt() throws Exception {
        //Given
        final EntityLayer1 entity = new EntityLayer1("layer1_nested", new Layer2("layer2", new Layer3("layer3")));
        manager.crud().insert(entity).execute();

        //When
        final TypedMap found = manager.dsl()
                .select()
                .layer()
                .layer2().layer()
                .layer2().layer3().allColumns()
                .fromBaseTable()
                .where()
                .layer().Eq(entity.getLayer())
                .getTypedMap();

        //Then
        assertThat(found).isNotNull();
        assertThat(found.<String>getTyped("layer")).isEqualTo(entity.getLayer());
        assertThat(found.<String>getTyped("layer2.layer")).isEqualTo(entity.getLayer2().getLayer());
        assertThat(found.<UDTValue>getTyped("layer2.layer3").getString("layer")).isEqualTo(entity.getLayer2().getLayer3().getLayer());
    }
}
