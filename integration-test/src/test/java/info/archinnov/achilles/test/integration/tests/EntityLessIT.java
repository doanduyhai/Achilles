package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.embedded.CassandraEmbeddedServer;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.entity.manager.PersistenceManagerFactory;
import info.archinnov.achilles.entity.manager.PersistenceManagerFactory.PersistenceManagerFactoryBuilder;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;

public class EntityLessIT {

	private static final String TEST_KEYSPACE = "test_keyspace";

	private Session session = CassandraEmbeddedServerBuilder.noEntityPackages().withKeyspaceName(TEST_KEYSPACE)
			.cleanDataFilesAtStartup(true).buildNativeSessionOnly();

	private PersistenceManager manager;

	@Before
	public void setUp() {
		PersistenceManagerFactory factory = PersistenceManagerFactoryBuilder.builder()
				.withConnectionContactPoints(DEFAULT_CASSANDRA_HOST).withCQLPort(CassandraEmbeddedServer.getCqlPort())
				.withCluster(session.getCluster()).withNativeSession(session).withKeyspaceName(TEST_KEYSPACE).build();
		manager = factory.createPersistenceManager();
	}

	@Test
	public void should_bootstrap_achilles_without_entity_package_for_native_query() throws Exception {
		Map<String, Object> keyspaceMap = manager.nativeQuery(
				"SELECT keyspace_name from system.schema_keyspaces where keyspace_name='system'").first();

		assertThat(keyspaceMap).hasSize(1);
		assertThat(keyspaceMap.get("keyspace_name")).isEqualTo("system");
	}

}
