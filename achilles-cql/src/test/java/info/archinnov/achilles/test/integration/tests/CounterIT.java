package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * CounterIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AchillesInternalCQLResource resource = new AchillesInternalCQLResource("CompleteBean", AchillesCounter.CQL_COUNTER_TABLE);

    private CQLEntityManager em = resource.getEm();

    private Session session = resource.getNativeSession();

    private CompleteBean bean;

    @Test
    public void should_persist_counter() throws Exception
    {
        bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

        bean = em.merge(bean);
        bean.getVersion().incr(2L);

        Row row = session.execute(
                "select counter_value from achilles_counter_table where fqcn='"
                        + CompleteBean.class.getCanonicalName()
                        + "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

        assertThat(row.getLong("counter_value")).isEqualTo(2L);
    }

    @Test
    public void should_find_counter() throws Exception
    {
        long version = 10L;
        bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

        bean = em.merge(bean);
        bean.getVersion().incr(version);

        assertThat(bean.getVersion().get()).isEqualTo(version);
    }

    @Test
    public void should_remove_counter() throws Exception
    {
        long version = 154321L;
        bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
        bean = em.merge(bean);
        bean.getVersion().incr(version);

        Row row = session.execute(
                "select counter_value from achilles_counter_table where fqcn='"
                        + CompleteBean.class.getCanonicalName()
                        + "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

        assertThat(row.getLong("counter_value")).isEqualTo(version);

        // Pause required to let Cassandra remove counter columns
        Thread.sleep(1000);

        em.remove(bean);

        row = session.execute(
                "select counter_value from achilles_counter_table where fqcn='"
                        + CompleteBean.class.getCanonicalName()
                        + "' and primary_key='" + bean.getId() + "' and property_name='version'").one();

        assertThat(row).isNull();
    }
}
