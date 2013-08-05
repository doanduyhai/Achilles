package info.archinnov.achilles.integration.spring;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.ImmutableMap;

/**
 * ThriftEntityManagerFactoryBeanTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerFactoryBeanTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock
    private Cluster cluster;

    @Mock
    private Keyspace keyspace;

    private ThriftEntityManagerFactoryBean factory;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ObjectMapperFactory objectMapperFactory;

    @Before
    public void setUp()
    {
        factory = new ThriftEntityManagerFactoryBean();
    }

    @Test
    public void should_exception_when_no_entity_packages() throws Exception
    {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Entity packages should be provided for entity scanning");
        factory.initialize();
    }

    @Test
    public void should_exception_when_blank_cassandra_host() throws Exception
    {
        factory.setEntityPackages("com.test");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Either a Cassandra cluster or hostname:port & clusterName should be provided");
        factory.initialize();
    }

    @Test
    public void should_exception_when_blank_cluster_name() throws Exception
    {
        factory.setEntityPackages("com.test");
        factory.setCassandraHost("localhost:9160");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Either a Cassandra cluster or hostname:port & clusterName should be provided");
        factory.initialize();
    }

    @Test
    public void should_exception_when_blank_keyspace_name() throws Exception
    {
        factory.setEntityPackages("com.test");
        factory.setCassandraHost("localhost:9160");
        factory.setClusterName("test");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Either a Cassandra keyspace or keyspaceName should be provided");
        factory.initialize();
    }

    @Test
    public void should_build_with_cluster_and_keyspace_objects() throws Exception
    {
        factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
        factory.setCluster(cluster);
        factory.setKeyspace(keyspace);

        exception.expect(AchillesException.class);

        factory.initialize();
    }

    @Test
    public void should_build_with_cluster_and_keyspace_names() throws Exception
    {
        factory.setEntityPackages("info.archinnov.achilles.test.integration.entity");
        factory.setCassandraHost("localhost:9160");
        factory.setClusterName("cluster");
        factory.setKeyspaceName("keyspace");
        factory.setObjectMapper(objectMapper);
        factory.setObjectMapperFactory(objectMapperFactory);
        factory.setConsistencyLevelReadDefault("ONE");
        factory.setConsistencyLevelWriteDefault("ONE");
        factory.setConsistencyLevelReadMap(ImmutableMap.of("entity", "ONE"));
        factory.setConsistencyLevelWriteMap(ImmutableMap.of("entity", "ONE"));
        factory.setEnsureJoinConsistency(true);
        factory.setForceColumnFamilyCreation(true);

        exception.expect(AchillesException.class);

        factory.initialize();
    }
}
