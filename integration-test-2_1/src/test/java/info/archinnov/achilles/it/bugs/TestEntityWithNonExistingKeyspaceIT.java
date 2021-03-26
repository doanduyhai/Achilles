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
import static java.lang.String.format;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Cluster;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.internals.entities.EntityWithNonExistingKeyspace;

public class TestEntityWithNonExistingKeyspaceIT {

    @Rule
    public ExpectedException expectException = ExpectedException.none();

    @Test
    public void should_fail_if_entity_keyspace_does_not_exist() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder
                .builder()
                .buildNativeCluster();

        //When

        expectException.expect(AchillesException.class);
        expectException.expectMessage(format("The keyspace %s defined on entity %s " +
                "does not exist in Cassandra", "non_existing", EntityWithNonExistingKeyspace.class.getCanonicalName()));

        ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithNonExistingKeyspace.class)
                .doForceSchemaCreation(false)
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                .build();


    }

}
