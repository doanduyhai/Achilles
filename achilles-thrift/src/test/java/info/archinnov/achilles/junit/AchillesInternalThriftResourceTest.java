package info.archinnov.achilles.junit;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.manager.ThriftEntityManagerFactory;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.User;
import java.util.List;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

public class AchillesInternalThriftResourceTest {

    @Rule
    public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(Steps.AFTER_TEST, "User");

    private Cluster cluster = resource.getCluster();
    private Keyspace keyspace = resource.getKeyspace();
    private ThriftEntityManagerFactory emf = resource.getFactory();
    private ThriftEntityManager em = resource.getEm();
    private ThriftConsistencyLevelPolicy policy = resource.getConsistencyPolicy();
    private ThriftGenericEntityDao dao = resource.getEntityDao("User", Long.class);

    @Test
    public void should_bootstrap_embedded_server_and_entity_manager() throws Exception
    {

        Long id = RandomUtils.nextLong();
        em.persist(new User(id, "fn", "ln"));

        List<Pair<Composite, Object>> columnsRange = dao.findColumnsRange(id, null, null, false, 100);

        assertThat(columnsRange).hasSize(3);

        String idName = columnsRange.get(0).left.get(1, STRING_SRZ);
        String idValue = (String) columnsRange.get(0).right;

        String firstnameName = columnsRange.get(1).left.get(1, STRING_SRZ);
        String firstnameValue = (String) columnsRange.get(1).right;

        String lastnameName = columnsRange.get(2).left.get(1, STRING_SRZ);
        String lastnameValue = (String) columnsRange.get(2).right;

        assertThat(idName).isEqualTo("id");
        assertThat(idValue).isEqualTo(id.toString());

        assertThat(firstnameName).isEqualTo("firstname");
        assertThat(firstnameValue).isEqualTo("fn");

        assertThat(lastnameName).isEqualTo("lastname");
        assertThat(lastnameValue).isEqualTo("ln");
    }

    @Test
    public void should_create_resources_once() throws Exception
    {
        AchillesInternalThriftResource resource = new AchillesInternalThriftResource();

        assertThat(resource.getCluster()).isSameAs(cluster);
        assertThat(resource.getKeyspace()).isSameAs(keyspace);
        assertThat(resource.getFactory()).isSameAs(emf);
        assertThat(resource.getEm()).isSameAs(em);
        assertThat(resource.getConsistencyPolicy()).isSameAs(policy);
    }
}
