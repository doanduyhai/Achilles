/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.embedded;

import static org.fest.assertions.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.persistence.PersistenceManagerFactory;

@RunWith(MockitoJUnitRunner.class)
public class CassandraEmbeddedServerBuilderTest {

    @Test
    public void should_bootstrap_only_one_instance_per_keyspace() throws Exception {

        PersistenceManagerFactory factory1 = CassandraEmbeddedServerBuilder
                .noEntityPackages()
                .withKeyspaceName("keyspace1")
                .buildPersistenceManagerFactory();

        PersistenceManagerFactory factory2 = CassandraEmbeddedServerBuilder
                .noEntityPackages()
                .withKeyspaceName("keyspace2")
                .buildPersistenceManagerFactory();

        PersistenceManagerFactory factory3 = CassandraEmbeddedServerBuilder
                .noEntityPackages()
                .withKeyspaceName("keyspace1")
                .buildPersistenceManagerFactory();

        final Cluster cluster = CassandraEmbeddedServerBuilder
                .noEntityPackages()
                .buildNativeClusterOnly();


        assertThat(factory1).isNotEqualTo(factory2);
        assertThat(factory1).isEqualTo(factory3);
        assertThat(factory1.createPersistenceManager().getNativeSession().getCluster()).isSameAs(cluster);
        assertThat(factory2.createPersistenceManager().getNativeSession().getCluster()).isSameAs(cluster);
        assertThat(factory3.createPersistenceManager().getNativeSession().getCluster()).isSameAs(cluster);
    }
}
