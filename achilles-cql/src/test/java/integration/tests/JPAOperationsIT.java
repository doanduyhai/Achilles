package integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CQLCassandraDaoTest;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
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
import com.datastax.driver.core.SimpleStatement;

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

		StringBuilder tableCompleteBeanTweets = new StringBuilder();
		tableCompleteBeanTweets.append("CREATE TABLE complete_bean_tweets(");
		tableCompleteBeanTweets.append("id bigint,");
		tableCompleteBeanTweets.append("key uuid,");
		tableCompleteBeanTweets.append("value text,");
		tableCompleteBeanTweets.append("primary key(id,key))");

		StringBuilder tableCompleteBeanUserTweets = new StringBuilder();
		tableCompleteBeanUserTweets.append("CREATE TABLE complete_bean_user_tweets(");
		tableCompleteBeanUserTweets.append("id bigint,");
		tableCompleteBeanUserTweets.append("user text,");
		tableCompleteBeanUserTweets.append("tweet uuid,");
		tableCompleteBeanUserTweets.append("value text,");
		tableCompleteBeanUserTweets.append("primary key(id,user,tweet))");

		StringBuilder tableCompleteBeanWideMap = new StringBuilder();
		tableCompleteBeanWideMap.append("CREATE TABLE complete_bean_widemap(");
		tableCompleteBeanWideMap.append("id bigint,");
		tableCompleteBeanWideMap.append("key int,");
		tableCompleteBeanWideMap.append("value text,");
		tableCompleteBeanWideMap.append("primary key(id,key))");

		StringBuilder tableCompleteBeanMultiKeyWideMap = new StringBuilder();
		tableCompleteBeanMultiKeyWideMap.append("CREATE TABLE complete_bean_multi_key_widemap(");
		tableCompleteBeanMultiKeyWideMap.append("id bigint,");
		tableCompleteBeanMultiKeyWideMap.append("user text,");
		tableCompleteBeanMultiKeyWideMap.append("tweet uuid,");
		tableCompleteBeanMultiKeyWideMap.append("value text,");
		tableCompleteBeanMultiKeyWideMap.append("primary key(id,user,tweet))");

		StringBuilder tableCompleteBeanPopularTopics = new StringBuilder();
		tableCompleteBeanPopularTopics.append("CREATE TABLE complete_bean_popular_topics(");
		tableCompleteBeanPopularTopics.append("id bigint,");
		tableCompleteBeanPopularTopics.append("key text,");
		tableCompleteBeanPopularTopics.append("value counter,");
		tableCompleteBeanPopularTopics.append("primary key(id,key))");

		session.execute(tableUser.toString());
		session.execute(tableTweet.toString());
		session.execute(tableCompleteBeanTweets.toString());
		session.execute(tableCompleteBeanUserTweets.toString());
		session.execute(tableCompleteBeanWideMap.toString());
		session.execute(tableCompleteBeanMultiKeyWideMap.toString());
		session.execute(tableCompleteBeanPopularTopics.toString());

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

	@Test
	public void should_return_managed_entity_after_merge() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();
		bean = em.merge(bean);

		assertThat(bean).isInstanceOf(Factory.class);
	}

	@Test
	public void should_return_same_entity_as_merged_bean_when_managed() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("Jonathan").buid();
		Tweet tweet = TweetTestBuilder.tweet().randomId().content("tweet").buid();
		bean.setWelcomeTweet(tweet);

		bean = em.merge(bean);

		CompleteBean bean2 = em.merge(bean);

		assertThat(bean2).isSameAs(bean);
		assertThat(bean.getWelcomeTweet()).isInstanceOf(Factory.class);
		assertThat(bean2.getWelcomeTweet()).isInstanceOf(Factory.class);
	}

	@Test
	public void should_remove() throws Exception
	{
		StringBuilder tableAchillesCounter = new StringBuilder();
		tableAchillesCounter.append("CREATE TABLE achilles_counter_table(");
		tableAchillesCounter.append("fqcn text,");
		tableAchillesCounter.append("primary_key text,");
		tableAchillesCounter.append("property_name text,");
		tableAchillesCounter.append("counter_value counter,");
		tableAchillesCounter.append("primary key((fqcn,primary_key),property_name))");

		session.execute(tableAchillesCounter.toString());

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

		bean = em.merge(bean);

		em.remove(bean);

		List<Row> rows = session
				.execute("select * from completebean where id=" + bean.getId())
				.all();
		assertThat(rows).isEmpty();
	}

	@Test(expected = IllegalStateException.class)
	public void should_exception_when_removing_transient_entity() throws Exception
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

		em.remove(bean);
	}

	@Test
	public void should_get_reference() throws Exception
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

		CompleteBean foundBean = em.getReference(CompleteBean.class, bean.getId());

		assertThat(foundBean).isNotNull();

		// Real object should be empty
		CompleteBean realObject = em.unproxy(foundBean);

		assertThat(realObject.getId()).isEqualTo(bean.getId());
		assertThat(realObject.getName()).isNull();
		assertThat(realObject.getAge()).isNull();
		assertThat(realObject.getFriends()).isNull();
		assertThat(realObject.getFollowers()).isNull();
		assertThat(realObject.getPreferences()).isNull();

		assertThat(foundBean.getId()).isEqualTo(bean.getId());
		assertThat(foundBean.getName()).isEqualTo("DuyHai");
		assertThat(foundBean.getAge()).isEqualTo(35L);
		assertThat(foundBean.getFriends()).containsExactly("foo", "bar");
		assertThat(foundBean.getFollowers()).contains("George", "Paul");

		assertThat(foundBean.getPreferences()).containsKey(1);
		assertThat(foundBean.getPreferences()).containsKey(2);
		assertThat(foundBean.getPreferences()).containsKey(3);

		assertThat(foundBean.getPreferences()).containsValue("FR");
		assertThat(foundBean.getPreferences()).containsValue("Paris");
		assertThat(foundBean.getPreferences()).containsValue("75014");
	}

	@Test
	public void should_exception_refreshing_non_managed_entity() throws Exception
	{
		CompleteBean completeBean = CompleteBeanTestBuilder
				.builder()
				.randomId()
				.name("name")
				.buid();
		exception.expect(IllegalStateException.class);
		exception.expectMessage("The entity '" + completeBean + "' is not in 'managed' state");
		em.refresh(completeBean);
	}

	@Test
	public void should_refresh() throws Exception
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

		bean = em.merge(bean);

		bean.getFriends();

		PropertyMeta<Void, String> nameMeta = new PropertyMeta<Void, String>();
		nameMeta.setType(PropertyType.SIMPLE);

		nameMeta.setPropertyName("name");

		session.execute("UPDATE completebean SET name='DuyHai_modified' WHERE id=" + bean.getId());
		session.execute("UPDATE completebean SET friends=friends + ['qux'] WHERE id="
				+ bean.getId());

		em.refresh(bean);

		assertThat(bean.getName()).isEqualTo("DuyHai_modified");
		assertThat(bean.getFriends()).hasSize(3);
		assertThat(bean.getFriends().get(2)).isEqualTo("qux");
	}

	@Test
	public void should_find_unmapped_field() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
				.label("label")
				.age(35L)
				.addFriends("foo", "bar")
				.addFollowers("George", "Paul")
				.addPreference(1, "FR")
				.addPreference(2, "Paris")
				.addPreference(3, "75014")
				.buid();

		bean = em.merge(bean);

		assertThat(bean.getLabel()).isEqualTo("label");
	}

	@Test
	public void should_return_null_and_not_wrapper_for_null_values() throws Exception
	{
		CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai") //
				.buid();

		bean.setFriends(null);
		bean.setFollowers(null);
		bean.setPreferences(null);

		em.persist(bean);

		bean = em.find(CompleteBean.class, bean.getId());

		assertThat(bean.getFriends()).isNull();
		assertThat(bean.getFollowers()).isNull();
		assertThat(bean.getPreferences()).isNull();
		assertThat(bean.getLabel()).isNull();
		assertThat(bean.getAge()).isNull();
	}

	@After
	public void tearDown()
	{
		String listAllTables = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='achilles'";
		List<Row> rows = session.execute(listAllTables).all();

		for (Row row : rows)
		{
			session
					.execute(new SimpleStatement("drop table " + row.getString("columnfamily_name")));
		}
	}
}
