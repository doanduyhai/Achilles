package info.archinnov.achilles.embedded;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

@RunWith(MockitoJUnitRunner.class)
public class CassandraEmbeddedServerBuilderTest {

    @Test
    public void should_start_new_embedded_server() throws Exception {
        //Given
        String keyspace = RandomStringUtils.randomAlphabetic(9);
        final Session session = CassandraEmbeddedServerBuilder.builder()
                .withKeyspaceName(keyspace)
                .buildNativeSession();

        //Then
        assertThat(session).isNotNull();
        final Row one = session.execute("SELECT * FROM system.local LIMIT 1").one();
        assertThat(one).isNotNull();
    }

}