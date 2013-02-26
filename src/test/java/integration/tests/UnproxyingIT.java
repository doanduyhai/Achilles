package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getDynamicCompositeDao;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;
import integration.tests.entity.TweetTestBuilder;
import net.sf.cglib.proxy.Factory;

import org.junit.After;
import org.junit.Test;

/**
 * UnproxyingIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class UnproxyingIT
{

	private GenericDynamicCompositeDao<Long> dao = getDynamicCompositeDao(LONG_SRZ,
			normalizerAndValidateColumnFamilyName(CompleteBean.class.getName()));

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	@Test
	public void should_unproxy_object() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
		Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
		bean.setWelcomeTweet(tweet);

		bean = em.merge(bean);

		bean = em.unproxy(bean);

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
		bean = em.unproxy(bean);

		assertThat(bean).isNotInstanceOf(Factory.class);
		assertThat(bean.getWelcomeTweet()).isNotInstanceOf(Factory.class);
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
