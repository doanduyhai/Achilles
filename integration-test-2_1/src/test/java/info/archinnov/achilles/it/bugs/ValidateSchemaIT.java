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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.Cluster;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.internals.entities.EntityWithMissingClustering;
import info.archinnov.achilles.internals.entities.EntityWithMissingPartitionKey;
import info.archinnov.achilles.internals.entities.EntityWithMissingStaticCol;

public class ValidateSchemaIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void should_fail_validating_schema_when_partition_key_missing() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder.builder()
                .withScript("EntityWithMissingPartitionKey/schema.cql")
                .buildNativeCluster();
        cluster.init();

        //When
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The mapped partition key(s) [id] for entity " +
                "info.archinnov.achilles.internals.entities.EntityWithMissingPartitionKey " +
                "do not correspond to live schema partition key(s) [id, bucket]");

        //Then
        ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithMissingPartitionKey.class)
                .build();
    }

    @Test
    public void should_fail_validating_schema_when_clustering_column_missing() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder.builder()
                .withScript("EntityWithMissingClustering/schema.cql")
                .buildNativeCluster();

        //When
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The mapped clustering column(s) [clust] for entity " +
                "info.archinnov.achilles.internals.entities.EntityWithMissingClustering " +
                "do not correspond to live schema clustering column(s) [clust, missing_clust]");

        //Then
        ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithMissingClustering.class)
                .build();
    }


    @Test
    public void should_fail_validating_schema_when_static_column_missing() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder.builder()
                .withScript("EntityWithMissingStaticCol/schema.cql")
                .buildNativeCluster();

        //When
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The mapped static column(s) [staticcol] for entity " +
                "info.archinnov.achilles.internals.entities.EntityWithMissingStaticCol " +
                "do not correspond to live schema static column(s) [missing_static, staticcol]");

        //Then
        ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithMissingStaticCol.class)
                .build();
    }
}
