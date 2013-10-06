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
package info.archinnov.achilles.integration.spring;

import info.archinnov.achilles.json.ObjectMapperFactory;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.Policies;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityManagerFactoryBeanTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLEntityManagerFactoryBean factory;

	@Mock
	private ObjectMapper objectMapper;

	@Mock
	private ObjectMapperFactory objectMapperFactory;

	@Before
	public void setUp() {
		factory = new CQLEntityManagerFactoryBean();
	}

	@Test
	public void should_exception_when_no_contact_point_set() throws Exception {
		factory.setEntityPackages("com.test");
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Either 'contactPoints/port/keyspace name' or 'cluster/session' should be provided");
		factory.initialize();
	}

	@Test
	public void should_exception_when_no_port_set() throws Exception {
		factory.setEntityPackages("com.test");
		factory.setContactPoints("localhost");

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Either 'contactPoints/port/keyspace name' or 'cluster/session' should be provided");
		factory.initialize();
	}

	@Test
	public void should_exception_when_no_keyspace_name_set() throws Exception {
		factory.setEntityPackages("com.test");
		factory.setContactPoints("localhost");
		factory.setPort(9160);

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Either 'contactPoints/port/keyspace name' or 'cluster/session' should be provided");
		factory.initialize();
	}

	@Test
	public void should_exception_when_no_ssl_options_when_ssl_is_enabled() throws Exception {
		factory.setEntityPackages("com.test");
		factory.setContactPoints("localhost");
		factory.setPort(9160);
		factory.setKeyspaceName("keyspace");
		factory.setSslEnabled(true);

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("'sslOptions' property should be set when SSL is enabled");
		factory.initialize();
	}

	@Test
	public void should_build_with_minimum_parameters() throws Exception {
		factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
		factory.setContactPoints("localhost");
		factory.setPort(65000);
		factory.setKeyspaceName("keyspace");

		exception.expect(NoHostAvailableException.class);

		factory.initialize();
	}

	@Test
	public void should_build_with_all_parameters() throws Exception {
		factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
		factory.setContactPoints("localhost");
		factory.setPort(65000);
		factory.setKeyspaceName("keyspace");
		factory.setCompression(Compression.SNAPPY);
		factory.setRetryPolicy(Policies.defaultRetryPolicy());
		factory.setLoadBalancingPolicy(Policies.defaultLoadBalancingPolicy());
		factory.setReconnectionPolicy(Policies.defaultReconnectionPolicy());
		factory.setUsername("username");
		factory.setPassword("password");
		factory.setDisableJmx(true);
		factory.setDisableMetrics(true);
		factory.setSslEnabled(true);
		factory.setSslOptions(new SSLOptions());

		factory.setObjectMapper(objectMapper);
		factory.setObjectMapperFactory(objectMapperFactory);
		factory.setConsistencyLevelReadDefault("ONE");
		factory.setConsistencyLevelWriteDefault("ONE");
		factory.setConsistencyLevelReadMap(ImmutableMap.of("entity", "ONE"));
		factory.setConsistencyLevelWriteMap(ImmutableMap.of("entity", "ONE"));
		factory.setForceColumnFamilyCreation(true);

		exception.expect(Exception.class);

		factory.initialize();
	}
}
