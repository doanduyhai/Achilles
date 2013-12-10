package info.archinnov.achilles.test.integration.tests;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
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

	@Test
	public void should_testname() throws Exception {
		System.out.println("Existing cql port = " + CassandraEmbeddedServer.getCqlPort());
		// PersistenceManagerFactory factory =
		// PersistenceManagerFactoryBuilder.builder()
		// .withConnectionContactPoints(DEFAULT_CASSANDRA_HOST).withCQLPort(CassandraEmbeddedServer.getCqlPort())
		// .withKeyspaceName("temp_test").build();
		// manager = factory.createPersistenceManager();
		Session session = manager.getNativeSession();
		session.execute("CREATE TABLE user(id bigint PRIMARY KEY,name text,description text,payload text)");

		Row row = session.execute("SELECT description FROM user WHERE id=" + 100L).one();
		assertThat(row).isNull();

		PreparedStatement insertPS = session.prepare(insertInto("user").value("id", bindMarker()).value("name",
				bindMarker()));
		PreparedStatement updatePS = session.prepare(update("user").with(set("description", bindMarker()))
				.and(set("payload", bindMarker())).where(eq("id", bindMarker())));
		BoundStatement insertBS = insertPS.bind(100L, "John DOE");
		BoundStatement updateBS = updatePS.bind("This is John DOE",
				"{'id':'446e7d4e-88b2-2d84-60d8-da6442ec88e3','content':'welcomeTweet'}", 100L);
		BatchStatement batch = new BatchStatement();
		batch.add(insertBS);
		batch.add(updateBS);

		session.execute(batch);

		Row row2 = session.execute("SELECT description FROM user WHERE id=" + 100L).one();
		assertThat(row2.getString("description")).isEqualTo("This is John DOE");
	}
}
