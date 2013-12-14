/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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

package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.embedded.CassandraEmbeddedServer;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.entity.manager.PersistenceManagerFactory;

@RunWith(MockitoJUnitRunner.class)
public class CQLEmbeddedServerIT {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private Session session = CassandraEmbeddedServerBuilder.noEntityPackages().withKeyspaceName("test_keyspace")
			.buildNativeSessionOnly();

	@Test
	public void should_return_same_native_session() throws Exception {
		Session session = CassandraEmbeddedServerBuilder.noEntityPackages().withKeyspaceName("test_keyspace")
				.buildNativeSessionOnly();

		assertThat(session).isSameAs(this.session);
	}

	@Test
	public void should_return_same_manager_for_same_keyspace() throws Exception {
		PersistenceManager manager1 = CassandraEmbeddedServerBuilder.noEntityPackages()
				.withKeyspaceName("second_keyspace").buildPersistenceManager();

		PersistenceManager manager2 = CassandraEmbeddedServerBuilder.noEntityPackages()
				.withKeyspaceName("second_keyspace").buildPersistenceManager();

		assertThat(manager1).isSameAs(manager2);
	}

	@Test
	public void should_return_same_factory_for_same_keyspace() throws Exception {
		PersistenceManagerFactory factory1 = CassandraEmbeddedServerBuilder.noEntityPackages()
				.withKeyspaceName("third_keyspace").buildPersistenceManagerFactory();

		PersistenceManagerFactory factory2 = CassandraEmbeddedServerBuilder.noEntityPackages()
				.withKeyspaceName("third_keyspace").buildPersistenceManagerFactory();

		assertThat(factory1).isSameAs(factory2);
	}

	@Test
	public void should_exception_when_embedded_already_started_with_another_cql_port() throws Exception {

        String cassandraHost = System.getProperty(CassandraEmbeddedServer.CASSANDRA_HOST);
        if(StringUtils.isBlank(cassandraHost)) {
            exception.expect(IllegalArgumentException.class);
            exception.expectMessage("An embedded Cassandra server is already listening to CQL port");
        }

		CassandraEmbeddedServerBuilder.noEntityPackages().withKeyspaceName("test_keyspace").withCQLPort(9500)
				.buildNativeSessionOnly();
	}

	@Test
	public void should_exception_when_embedded_already_started_with_another_thrift_port() throws Exception {

        String cassandraHost = System.getProperty(CassandraEmbeddedServer.CASSANDRA_HOST);
        if(StringUtils.isBlank(cassandraHost)) {
            exception.expect(IllegalArgumentException.class);
            exception.expectMessage("An embedded Cassandra server is already listening to Thrift port");
        }

		CassandraEmbeddedServerBuilder.noEntityPackages().withKeyspaceName("test_keyspace").withThriftPort(9500)
				.buildNativeSessionOnly();
	}
}
