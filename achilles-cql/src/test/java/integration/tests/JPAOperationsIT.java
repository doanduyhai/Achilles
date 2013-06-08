package integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import net.sf.cglib.proxy.Factory;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import testBuilders.TweetTestBuilder;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * JPAOperationsIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class JPAOperationsIT
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private Session session = CQLCassandraDaoTest.getCqlSession();

	private CQLEntityManager em = CQLCassandraDaoTest.getEm();

	private ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		StringBuilder tableUser = new StringBuilder();
		tableUser.append("CREATE TABLE completebean(");
		tableUser.append("id bigint,");
		tableUser.append("name text,");
		tableUser.append("label text,");
		tableUser.append("age_in_years bigint,");
		tableUser.append("welcometweet uuid,");
		tableUser.append("friends list<text>,");
		tableUser.append("followers set<text>,");
		tableUser.append("preferences map<int,text>,");
		tableUser.append("primary key(id))");

		StringBuilder tableTweet = new StringBuilder();
		tableTweet.append("CREATE TABLE tweet(");
		tableTweet.append("id uuid,");
		tableTweet.append("creator bigint,");
		tableTweet.append("content text,");
		tableTweet.append("primary key(id))");

		session.execute(tableUser.toString());
		session.execute(tableTweet.toString());
	}

	@Test
	public void should_persist() throws Exception
	{

		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("DuyHai")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		em.persist(bean);

		Row row = session.execute(
				"select name,age_in_years,friends,followers,preferences from completebean where id = "
						+ bean.getId()).one();

		assertThat(row.getString("name")).isEqualTo("DuyHai");
		assertThat(row.getLong("age_in_years")).isEqualTo(35L);
		assertThat(row.getList("friends", String.class)).containsExactly("foo", "bar");
		assertThat(row.getSet("followers", String.class)).containsOnly("George", "Paul");

		Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);

		assertThat(preferences).containsKey(1);
		assertThat(preferences).containsKey(2);
		assertThat(preferences).containsKey(3);

		assertThat(preferences).containsValue("FR");
		assertThat(preferences).containsValue("Paris");
		assertThat(preferences).containsValue("75014");

	}

	@Test
	public void should_persist_empty_bean() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		assertThat(found).isNotNull();
		assertThat(found.getId()).isEqualTo(bean.getId());
	}

	@Test
	public void should_cascade_merge_join_simple_property() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
		Tweet welcomeTweet = TweetTestBuilder.tweet().randomId().content("Welcome").buid();

		bean.setWelcomeTweet(welcomeTweet);

		em.merge(bean);

		Tweet persistedWelcomeTweet = em.find(Tweet.class, welcomeTweet.getId());

		assertThat(persistedWelcomeTweet).isNotNull();
		assertThat(persistedWelcomeTweet.getContent()).isEqualTo("Welcome");

		CompleteBean persistedBean = em.find(CompleteBean.class, bean.getId());

		assertThat(persistedBean).isNotNull();
		assertThat(persistedBean.getName()).isEqualTo("DuyHai");
	}

	@Test
	public void should_find() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		assertThat(found).isNotNull();
		assertThat(found).isInstanceOf(Factory.class);
	}

	@Test
	public void should_find_lazy_simple() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("Jonathan")
				.label("label")
				.buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		Factory factory = (Factory) found;
		CQLEntityInterceptor<CompleteBean> interceptor = (CQLEntityInterceptor<CompleteBean>) factory
				.getCallback(0);

		Method getLabel = CompleteBean.class.getDeclaredMethod("getLabel");
		String label = (String) getLabel.invoke(interceptor.getTarget());

		assertThat(label).isNull();

		String lazyLabel = found.getLabel();

		assertThat(lazyLabel).isNotNull();
		assertThat(lazyLabel).isEqualTo("label");
	}

	@Test
	public void should_find_lazy_list() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("Jonathan")
				.age(40L)
				.addFriends("bob", "alice")
				.addFollowers("Billy", "Stephen", "Jacky")
				.addPreference(1, "US")
				.addPreference(2, "New York")
				.buid();

		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		Factory factory = (Factory) found;
		CQLEntityInterceptor<CompleteBean> interceptor = (CQLEntityInterceptor<CompleteBean>) factory
				.getCallback(0);

		Method getFriends = CompleteBean.class.getDeclaredMethod("getFriends", (Class<?>[]) null);
		List<String> lazyFriends = (List<String>) getFriends.invoke(interceptor.getTarget());

		assertThat(lazyFriends).isNull();

		List<String> friends = found.getFriends();

		assertThat(friends).isNotNull();
		assertThat(friends).hasSize(2);
		assertThat(friends).containsExactly("bob", "alice");
	}

	@Test
	public void should_merge_modifications() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("Jonathan")
				.age(40L)
				.addFriends("bob", "alice")
				.addFollowers("Billy", "Stephen", "Jacky")
				.addPreference(1, "US")
				.addPreference(2, "New York")
				.buid();
		em.persist(bean);

		CompleteBean found = em.find(CompleteBean.class, bean.getId());

		found.setAge(100L);
		found.getFriends().add("eve");
		found.getPreferences().put(1, "FR");

		CompleteBean merged = em.merge(found);

		assertThat(merged).isSameAs(found);

		assertThat(merged.getFriends()).hasSize(3);
		assertThat(merged.getFriends()).containsExactly("bob", "alice", "eve");
		assertThat(merged.getPreferences()).hasSize(2);
		assertThat(merged.getPreferences().get(1)).isEqualTo("FR");

		Row row = session.execute("select * from completebean where id=" + bean.getId()).one();

		assertThat(row.getLong("age_in_years")).isEqualTo(100L);
		assertThat(row.getList("friends", String.class)).containsExactly("bob", "alice", "eve");
		Map<Integer, String> preferences = row.getMap("preferences", Integer.class, String.class);
		assertThat(preferences.get(1)).isEqualTo("FR");
		assertThat(preferences.get(2)).isEqualTo("New York");

	}

	@After
	public void tearDown()
	{
		session.execute("truncate completebean");
		session.execute("truncate tweet");
	}
}
