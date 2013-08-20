package info.archinnov.achilles.junit;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.manager.CQLEntityManagerFactory;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.User;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class AchillesInternalCQLResourceTest {
    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "User");

    private CQLEntityManagerFactory emf = resource.getFactory();
    private CQLEntityManager em = resource.getEm();
    private Session session = resource.getNativeSession();

    @Test
    public void should_bootstrap_embedded_server_and_entity_manager() throws Exception
    {

        Long id = RandomUtils.nextLong();
        em.persist(new User(id, "fn", "ln"));

        Row row = session.execute("SELECT * FROM User WHERE id=" + id).one();

        assertThat(row).isNotNull();

        assertThat(row.getString("firstname")).isEqualTo("fn");
        assertThat(row.getString("lastname")).isEqualTo("ln");
    }

    @Test
    public void should_create_resources_once() throws Exception
    {
        AchillesInternalCQLResource resource = new AchillesInternalCQLResource();

        assertThat(resource.getFactory()).isSameAs(emf);
        assertThat(resource.getEm()).isSameAs(em);
        assertThat(resource.getNativeSession()).isSameAs(session);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_exception_when_null_entity_package_provided() throws Exception
    {
        new AchillesCQLResource(null);
    }
}
