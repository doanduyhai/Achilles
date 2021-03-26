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

import java.nio.ByteBuffer;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.generated.ManagerFactory;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.generated.manager.EntityWithByteBufferType_Manager;
import info.archinnov.achilles.internals.entities.EntityWithByteBufferType;

public class TestEntityWithByteBufferTypeIT {

    @Test
    public void should_support_bytebuffer_type() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder
                .builder()
                .buildNativeCluster();

        final Long id = RandomUtils.nextLong(0, Long.MAX_VALUE);
        final byte[] bytes = RandomUtils.nextBytes(100);
        final ByteBuffer value = ByteBuffer.wrap(bytes);

        //When
        final ManagerFactory managerFactory = ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithByteBufferType.class)
                .doForceSchemaCreation(true)
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                .build();

        final EntityWithByteBufferType_Manager manager = managerFactory
                .forEntityWithByteBufferType();

        final EntityWithByteBufferType entity = new EntityWithByteBufferType(id, value);

        manager.crud().insert(entity).execute();

        final Row one = manager.getNativeSession().execute("SELECT * FROM " + EntityWithByteBufferType.TABLE +
                " WHERE id = " + id).one();

        assertThat(one).isNotNull();
        assertThat(one.getBytes("value")).isEqualTo(value);
    }

}
