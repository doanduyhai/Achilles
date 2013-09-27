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
package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.exception.AchillesException;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.Policies;

@RunWith(MockitoJUnitRunner.class)
public class CQLArgumentExtractorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private Cluster cluster;

	@Mock
	private Session session;

	private CQLArgumentExtractor extractor = new CQLArgumentExtractor();

	@Test(expected = Exception.class)
	public void should_init_cluster_with_all_params() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CONNECTION_CONTACT_POINTS_PARAM, "localhost");
		params.put(CONNECTION_PORT_PARAM, 9111);
		params.put(COMPRESSION_TYPE, Compression.SNAPPY);
		params.put(RETRY_POLICY, Policies.defaultRetryPolicy());
		params.put(LOAD_BALANCING_POLICY, Policies.defaultLoadBalancingPolicy());
		params.put(RECONNECTION_POLICY, Policies.defaultReconnectionPolicy());
		params.put(USERNAME, "user");
		params.put(PASSWORD, "pass");
		params.put(DISABLE_JMX, true);
		params.put(DISABLE_METRICS, true);
		params.put(SSL_ENABLED, true);
		params.put(SSL_OPTIONS, new SSLOptions());

		extractor.initCluster(params);
	}

	@Test
	public void should_get_cluster_directly_from_parameter() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CLUSTER_PARAM, cluster);

		Cluster actual = extractor.initCluster(params);
		assertThat(actual).isSameAs(cluster);
	}

	@Test(expected = NoHostAvailableException.class)
	public void should_init_cluster_with_minimum_params() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CONNECTION_CONTACT_POINTS_PARAM, "localhost");
		params.put(CONNECTION_PORT_PARAM, 9111);

		extractor.initCluster(params);
	}

	@Test
	public void should_exception_when_no_hostname_property() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();

		exception.expect(AchillesException.class);
		exception.expectMessage(CONNECTION_CONTACT_POINTS_PARAM
				+ " property should be provided");

		extractor.initCluster(params);
	}

	@Test
	public void should_exception_when_no_port_property() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CONNECTION_CONTACT_POINTS_PARAM, "localhost");

		exception.expect(AchillesException.class);
		exception.expectMessage(CONNECTION_PORT_PARAM
				+ " property should be provided");

		extractor.initCluster(params);
	}

	@Test
	public void should_init_session() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(KEYSPACE_NAME_PARAM, "achilles");

		when(cluster.connect("achilles")).thenReturn(session);

		Session actual = extractor.initSession(cluster, params);

		assertThat(actual).isSameAs(session);
	}

	@Test
	public void should_exception_when_no_keyspace_name_param() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();

		exception.expect(AchillesException.class);
		exception.expectMessage(KEYSPACE_NAME_PARAM
				+ " property should be provided");

		extractor.initSession(cluster, params);
	}

	@Test
	public void should_get_native_session_from_parameter() throws Exception {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(NATIVE_SESSION_PARAM, session);

		Session actual = extractor.initSession(cluster, params);

		assertThat(actual).isSameAs(session);
	}
}
