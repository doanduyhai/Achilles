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

import java.util.Date;

import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityAsChild_Manager;
import info.archinnov.achilles.generated.manager.EntityLayer1_Manager;
import info.archinnov.achilles.generated.manager.EntityWithStaticAnnotations_Manager;
import info.archinnov.achilles.generated.manager.SimpleEntity_Manager;
import info.archinnov.achilles.internals.entities.*;
import info.archinnov.achilles.internals.query.crud.FindWithOptions;
import info.archinnov.achilles.it.utils.CassandraLogAsserter;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.junit.AchillesTestResourceBuilder;

@RunWith(MockitoJUnitRunner.class)
public class MultiLayerNestingIT {


   @Test
   public void should_handle_3_levels_of_nesting_udt() throws Exception {
       //Given
       final Cluster cluster = CassandraEmbeddedServerBuilder
               .builder()
               .withScript("functions/createFunctions.cql")
               .buildNativeCluster();

       final ManagerFactory managerFactory = ManagerFactoryBuilder
               .builder(cluster)
               .withManagedEntityClasses(EntityLayer1.class)
               .doForceSchemaCreation(true)
               .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
               .build();

       final EntityLayer1_Manager manager = managerFactory.forEntityLayer1();
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
}
