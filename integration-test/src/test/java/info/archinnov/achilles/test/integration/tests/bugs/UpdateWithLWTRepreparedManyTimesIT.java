package info.archinnov.achilles.test.integration.tests.bugs;

import com.datastax.driver.core.Cluster;
import info.archinnov.achilles.junit.AchillesTestResource;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import org.junit.Rule;
import org.junit.Test;

import static info.archinnov.achilles.options.OptionsBuilder.ifEqualCondition;

public class UpdateWithLWTRepreparedManyTimesIT {

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(AchillesTestResource.Steps.BOTH, CompleteBean.TABLE_NAME);

    private PersistenceManager manager = resource.getPersistenceManager();

    private CassandraLogAsserter logAsserter = new CassandraLogAsserter();

    @Test
    public void should_not_re_prepare_updates_with_lwt() throws Exception {
        //Given
        final CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("John").buid();
        manager.insert(bean);

        final CompleteBean proxy1 = manager.forUpdate(CompleteBean.class, bean.getId());
        proxy1.setName("Helen");

        manager.update(proxy1, ifEqualCondition("name", "John"));

        //When
        final CompleteBean proxy2 = manager.forUpdate(CompleteBean.class, bean.getId());
        proxy2.setName("Richard");

        //Then
        logAsserter.prepareLogLevel(Cluster.class.getPackage().getName());
        manager.update(proxy2, ifEqualCondition("name", "Helen"));
        logAsserter.assertNotContains("Re-preparing already prepared query");
    }
}
