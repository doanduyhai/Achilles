package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    private ThriftCounterDao thriftCounterDao = ThriftCassandraDaoTest.getCounterDao();

    private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();
    private CompleteBean bean;

    @Before
    public void setUp()
    {
        thriftCounterDao.truncateCounters();
    }

    @Test
    public void should_persist_counter() throws Exception
    {
        bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

        bean = em.merge(bean);
        bean.getVersion().incr(2L);

        Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
        Composite comp = createCounterName("version");
        Long actual = thriftCounterDao.getCounterValue(keyComp, comp);

        assertThat(actual).isEqualTo(2L);
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
        Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
        Composite comp = createCounterName("version");
        Long actual = thriftCounterDao.getCounterValue(keyComp, comp);

        assertThat(actual).isEqualTo(version);

        // Pause required to let Cassandra remove counter columns
        Thread.sleep(1000);

        em.remove(bean);

        actual = thriftCounterDao.getCounterValue(keyComp, comp);

        assertThat(actual).isEqualTo(0);
    }

    private <T> Composite createCounterKey(Class<T> clazz, Long id)
    {
        Composite comp = new Composite();
        comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
        comp.setComponent(1, id.toString(), STRING_SRZ);
        return comp;
    }

    private Composite createCounterName(String propertyName)
    {
        Composite composite = new Composite();
        composite.addComponent(0, propertyName, ComponentEquality.EQUAL);
        return composite;
    }

}
