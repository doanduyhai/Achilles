package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import net.sf.cglib.proxy.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * LazyLoadingIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class LazyLoadingIT
{
    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    private CompleteBean bean;

    @Before
    public void setUp()
    {
        bean = CompleteBeanTestBuilder
                .builder()
                .randomId()
                .name("DuyHai")
                .age(35L)
                .addFriends("foo", "bar")
                .label("label")
                .buid();

        em.persist(bean);
    }

    @Test
    public void should_not_load_lazy_fields() throws Exception
    {
        bean = em.find(CompleteBean.class, bean.getId());

        Factory proxy = (Factory) bean;
        CQLEntityInterceptor<?> interceptor = (CQLEntityInterceptor<?>) proxy.getCallback(0);
        CompleteBean trueBean = (CompleteBean) interceptor.getTarget();

        assertThat(trueBean.getLabel()).isNull();
        assertThat(trueBean.getFriends()).isNull();

        // Trigger loading of lazy fields
        assertThat(bean.getLabel()).isEqualTo("label");
        assertThat(bean.getFriends()).containsExactly("foo", "bar");

        assertThat(trueBean.getLabel()).isEqualTo("label");
        assertThat(trueBean.getFriends()).containsExactly("foo", "bar");
    }

    @Test
    public void should_set_lazy_field() throws Exception
    {
        bean = em.find(CompleteBean.class, bean.getId());

        bean.setLabel("newLabel");

        assertThat(bean.getLabel()).isEqualTo("newLabel");
    }

    @After
    public void tearDown()
    {
        truncateTable("CompleteBean");
    }
}
