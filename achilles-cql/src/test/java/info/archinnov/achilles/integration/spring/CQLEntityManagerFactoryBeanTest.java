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

/**
 * CQLEntityManagerFactoryBeanTest
 * 
 * @author DuyHai DOAN
 * 
 */

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
    public void setUp()
    {
        factory = new CQLEntityManagerFactoryBean();
    }

    @Test
    public void should_exception_when_no_entity_packages() throws Exception
    {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("'entityPackages' should be provided for entity scanning");
        factory.initialize();
    }

    @Test
    public void should_exception_when_no_contact_point_set() throws Exception
    {
        factory.setEntityPackages("com.test");
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("'contactPoints' and 'port' for Cassandra connection should be provided");
        factory.initialize();
    }

    @Test
    public void should_exception_when_no_port_set() throws Exception
    {
        factory.setEntityPackages("com.test");
        factory.setContactPoints("localhost");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("'contactPoints' and 'port' for Cassandra connection should be provided");
        factory.initialize();
    }

    @Test
    public void should_exception_when_no_keyspace_name_set() throws Exception
    {
        factory.setEntityPackages("com.test");
        factory.setContactPoints("localhost");
        factory.setPort(9160);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("'keyspaceName' for Cassandra connection should be provided");
        factory.initialize();
    }

    @Test
    public void should_exception_when_no_ssl_options_when_ssl_is_enabled() throws Exception
    {
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
    public void should_build_with_minimum_parameters() throws Exception
    {
        factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
        factory.setContactPoints("localhost");
        factory.setPort(65000);
        factory.setKeyspaceName("keyspace");

        exception.expect(NoHostAvailableException.class);

        factory.initialize();
    }

    @Test
    public void should_build_with_all_parameters() throws Exception
    {
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
        factory.setEnsureJoinConsistency(true);
        factory.setForceColumnFamilyCreation(true);

        exception.expect(Exception.class);

        factory.initialize();
    }
}
