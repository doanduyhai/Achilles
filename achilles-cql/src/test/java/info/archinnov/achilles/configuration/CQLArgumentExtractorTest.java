package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.CQLConfigurationParameters.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
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
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * CQLArgumentExtractorTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CQLArgumentExtractorTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private Cluster cluster;

	@Mock
	private Session session;

	private CQLArgumentExtractor extractor = new CQLArgumentExtractor();

	@Test(expected = NoHostAvailableException.class)
	public void should_init_cluster() throws Exception
	{
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CONNECTION_CONTACT_POINTS_PARAM, "localhost");
		params.put(CONNECTION_PORT_PARAM, "9111");

		extractor.initCluster(params);
	}

	@Test
	public void should_exception_when_no_hostname_property() throws Exception
	{
		Map<String, Object> params = new HashMap<String, Object>();

		exception.expect(AchillesException.class);
		exception.expectMessage(CONNECTION_CONTACT_POINTS_PARAM + " property should be provided");

		extractor.initCluster(params);
	}

	@Test
	public void should_exception_when_no_port_property() throws Exception
	{
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CONNECTION_CONTACT_POINTS_PARAM, "localhost");

		exception.expect(AchillesException.class);
		exception.expectMessage(CONNECTION_PORT_PARAM + " property should be provided");

		extractor.initCluster(params);
	}

	@Test
	public void should_exception_when_port_is_not_a_number() throws Exception
	{
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(CONNECTION_CONTACT_POINTS_PARAM, "localhost");
		params.put(CONNECTION_PORT_PARAM, "12s");

		exception.expect(AchillesException.class);
		exception.expectMessage(CONNECTION_PORT_PARAM + " property should be a number");

		extractor.initCluster(params);
	}

	@Test
	public void should_init_session() throws Exception
	{
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(KEYSPACE_NAME_PARAM, "achilles");

		when(cluster.connect("achilles")).thenReturn(session);

		Session actual = extractor.initSession(cluster, params);

		assertThat(actual).isSameAs(session);
	}

	@Test
	public void should_exception_when_no_keyspace_name_param() throws Exception
	{
		Map<String, Object> params = new HashMap<String, Object>();

		exception.expect(AchillesException.class);
		exception.expectMessage(KEYSPACE_NAME_PARAM + " property should be provided");

		extractor.initSession(cluster, params);

	}
}
