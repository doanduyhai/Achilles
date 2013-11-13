package info.archinnov.achilles.embedded;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.entity.manager.CQLPersistenceManagerFactory;

@RunWith(MockitoJUnitRunner.class)
public class CQLEmbeddedServerBuilderTest {

    @Test
    public void should_bootstrap_only_one_instance_per_keyspace() throws Exception {

        CQLPersistenceManagerFactory factory1 = CQLEmbeddedServerBuilder
                .noEntityPackages()
                .withKeyspaceName("keyspace1")
                .buildPersistenceManagerFactory();

        CQLPersistenceManagerFactory factory2 = CQLEmbeddedServerBuilder
                .noEntityPackages()
                .withKeyspaceName("keyspace2")
                .buildPersistenceManagerFactory();

        CQLPersistenceManagerFactory factory3 = CQLEmbeddedServerBuilder
                .noEntityPackages()
                .withKeyspaceName("keyspace1")
                .buildPersistenceManagerFactory();


        assertThat(factory1).isNotEqualTo(factory2);
        assertThat(factory1).isEqualTo(factory3);
    }

}
