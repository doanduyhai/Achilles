package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.CQLCassandraDaoTest.truncateTable;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.test.builders.TweetTestBuilder;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import net.sf.cglib.proxy.Factory;
import org.junit.After;
import org.junit.Test;

/**
 * UnwrapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class UnwrapIT
{

    private CQLEntityManager em = CQLCassandraDaoTest.getEm();

    @Test
    public void should_unproxy_object() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
        Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
        bean.setWelcomeTweet(tweet);

        bean = em.merge(bean);

        bean = em.unwrap(bean);

        assertThat(bean).isNotInstanceOf(Factory.class);
    }

    @Test
    public void should_unproxy_directly_attached_join_object() throws Exception
    {
        CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
        Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
        bean.setWelcomeTweet(tweet);

        em.persist(bean);

        bean = em.find(CompleteBean.class, bean.getId());
        em.initialize(bean);
        bean = em.unwrap(bean);

        assertThat(bean).isNotInstanceOf(Factory.class);
        assertThat(bean.getWelcomeTweet()).isNotInstanceOf(Factory.class);
    }

    @After
    public void tearDown()
    {
        truncateTable("CompleteBean");
        truncateTable("Tweet");
    }
}
