package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import org.junit.Before;
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

    private Session session = CQLCassandraDaoTest.getCqlSession();

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    private CompleteBean bean;

    @Before
    public void setUp()
    {
        truncateTable("CompleteBean");
    }

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
