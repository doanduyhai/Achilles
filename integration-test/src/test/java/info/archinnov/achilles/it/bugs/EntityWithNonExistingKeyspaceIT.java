package info.archinnov.achilles.it.bugs;


import static info.archinnov.achilles.embedded.CassandraEmbeddedConfigParameters.DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME;
import static java.lang.String.format;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.generated.ManagerFactoryBuilder;
import info.archinnov.achilles.internals.entities.EntityWithNonExistingKeyspace;

@RunWith(MockitoJUnitRunner.class)
public class EntityWithNonExistingKeyspaceIT {

    @Rule
    public ExpectedException expectException = ExpectedException.none();

    @Test
    public void should_fail_if_entity_keyspace_does_not_exist() throws Exception {
        //Given
        final Cluster cluster = CassandraEmbeddedServerBuilder
                .builder()
                .buildNativeCluster();

        //When

        expectException.expect(AchillesException.class);
        expectException.expectMessage(format("The keyspace {} defined on entity {} " +
                "does not exist in Cassandra", "non_existing", EntityWithNonExistingKeyspace.class.getCanonicalName()));

        ManagerFactoryBuilder
                .builder(cluster)
                .withManagedEntityClasses(EntityWithNonExistingKeyspace.class)
                .doForceSchemaCreation(false)
                .withDefaultKeyspaceName(DEFAULT_CASSANDRA_EMBEDDED_KEYSPACE_NAME)
                .build();


    }

}
