package integration.tests;

import static info.archinnov.achilles.entity.metadata.PropertyType.COUNTER;
import static info.archinnov.achilles.entity.metadata.PropertyType.EXTERNAL_WIDE_MAP_COUNTER;
import static info.archinnov.achilles.entity.type.WideMap.BoundingMode.INCLUSIVE_BOUNDS;
import static info.archinnov.achilles.entity.type.WideMap.BoundingMode.INCLUSIVE_START_BOUND_ONLY;
import static info.archinnov.achilles.entity.type.WideMap.OrderingMode.ASCENDING;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;
import integration.tests.entity.Tweet;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.codehaus.jackson.map.ObjectMapper;
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

	private CounterDao counterDao = CassandraDaoTest.getCounterDao();
	private ThriftEntityManager em = CassandraDaoTest.getEm();
	private CompleteBean bean;
	private ObjectMapper mapper = new ObjectMapper();

	@Test
	public void should_persist_counter() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean.setVersion(2L);

		em.persist(bean);
		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(2L);
	}

	@Test
	public void should_merge_counter() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		bean.setVersion(251L);

		em.merge(bean);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(251L);
	}

	@Test
	public void should_find_counter() throws Exception
	{
		long version = 10L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(version).buid();

		em.persist(bean);

		bean = em.find(CompleteBean.class, bean.getId());

		assertThat(bean.getVersion()).isEqualTo(version);
	}

	@Test
	public void should_remove_counter() throws Exception
	{
		long version = 154321L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(version).buid();
		bean = em.merge(bean);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(version);

		em.remove(bean);

		actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(0);
	}

	@Test
	public void should_refresh_counter() throws Exception
	{
		long version = 454L, newVersion = -1234L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").version(version).buid();
		bean = em.merge(bean);

		assertThat(bean.getVersion()).isEqualTo(version);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(COUNTER, "version");

		counterDao.insertCounter(keyComp, comp, newVersion);
		em.refresh(bean);

		assertThat(bean.getVersion()).isEqualTo(newVersion);
	}

	@Test
	public void should_cascade_persist_counter() throws Exception
	{
		long favoriteCount = 120L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		Tweet tweet = createTweet();
		tweet.setFavoriteCount(favoriteCount);
		bean.setWelcomeTweet(tweet);

		em.persist(bean);

		Composite keyComp = createCounterKey(Tweet.class, tweet.getId());
		DynamicComposite comp = createCounterName(COUNTER, "favoriteCount");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(favoriteCount);
	}

	@Test
	public void should_cascade_merge_counter() throws Exception
	{
		long favoriteCount = 45648L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		Tweet tweet = createTweet();
		tweet.setFavoriteCount(favoriteCount);
		bean.setWelcomeTweet(tweet);

		em.merge(bean);

		Composite keyComp = createCounterKey(Tweet.class, tweet.getId());
		DynamicComposite comp = createCounterName(COUNTER, "favoriteCount");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(favoriteCount);
	}

	@Test
	public void should_find_counter_after_cascade_persist() throws Exception
	{
		long favoriteCount = 78L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		Tweet tweet = createTweet();
		tweet.setFavoriteCount(favoriteCount);
		bean.setWelcomeTweet(tweet);

		em.merge(bean);

		tweet = em.find(Tweet.class, tweet.getId());

		assertThat(tweet.getFavoriteCount()).isEqualTo(favoriteCount);
	}

	@Test
	public void should_cascade_refresh_counter() throws Exception
	{
		long favoriteCount = 10L, newFavoriteCount = 5465L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		Tweet tweet = createTweet();
		tweet.setFavoriteCount(favoriteCount);
		bean.setWelcomeTweet(tweet);

		bean = em.merge(bean);

		Composite keyComp = createCounterKey(Tweet.class, tweet.getId());
		DynamicComposite comp = createCounterName(COUNTER, "favoriteCount");

		counterDao.insertCounter(keyComp, comp, newFavoriteCount);

		em.refresh(bean);

		assertThat(bean.getWelcomeTweet().getFavoriteCount()).isEqualTo(newFavoriteCount);
	}

	@Test
	public void should_insert_counter_widemap() throws Exception
	{
		long javaCount = 1234, cassandraCount = 567;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = em.merge(bean);

		bean.getPopularTopics().insert("cassandra", cassandraCount);
		bean.getPopularTopics().insert("java", javaCount);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics",
				"java");

		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(javaCount);

		comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics", "cassandra");

		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(cassandraCount);
	}

	@Test
	public void should_find_counter_widemap() throws Exception
	{
		long javaCount = 655, cassandraCount = 5464, scalaCount = 1231;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Long> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", cassandraCount);
		popularTopics.insert("java", javaCount);
		popularTopics.insert("scala", scalaCount);

		List<KeyValue<String, Long>> foundKeyValues = popularTopics.find("cassandra", null, 10,
				INCLUSIVE_BOUNDS, ASCENDING);

		assertThat(foundKeyValues).hasSize(3);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo("cassandra");
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo(cassandraCount);
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo("java");
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo(javaCount);
		assertThat(foundKeyValues.get(2).getKey()).isEqualTo("scala");
		assertThat(foundKeyValues.get(2).getValue()).isEqualTo(scalaCount);

		List<String> foundKeys = popularTopics.findKeys("cassandra", null, 10, INCLUSIVE_BOUNDS,
				ASCENDING);

		assertThat(foundKeys).hasSize(3);
		assertThat(foundKeys.get(0)).isEqualTo("cassandra");
		assertThat(foundKeys.get(1)).isEqualTo("java");
		assertThat(foundKeys.get(2)).isEqualTo("scala");

		List<Long> foundValues = popularTopics.findValues("cassandra", null, 10, INCLUSIVE_BOUNDS,
				ASCENDING);

		assertThat(foundValues).hasSize(3);
		assertThat(foundValues.get(0)).isEqualTo(cassandraCount);
		assertThat(foundValues.get(1)).isEqualTo(javaCount);
		assertThat(foundValues.get(2)).isEqualTo(scalaCount);
	}

	@Test
	public void should_iterate_on_counter_widemap() throws Exception
	{
		long javaCount = 4564, cassandraCount = 873, scalaCount = 321, groovyCount = 54364, //
		hibernateCount = 545365464, springCount = 845143654;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Long> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", cassandraCount);
		popularTopics.insert("groovy", groovyCount);
		popularTopics.insert("hibernate", hibernateCount);
		popularTopics.insert("java", javaCount);
		popularTopics.insert("scala", scalaCount);
		popularTopics.insert("spring", springCount);

		Iterator<KeyValue<String, Long>> iterator = popularTopics.iterator("groovy", "spring", 2,
				INCLUSIVE_START_BOUND_ONLY, ASCENDING);

		KeyValue<String, Long> groovy = iterator.next();
		assertThat(groovy.getKey()).isEqualTo("groovy");
		assertThat(groovy.getValue()).isEqualTo(groovyCount);

		KeyValue<String, Long> hibernate = iterator.next();
		assertThat(hibernate.getKey()).isEqualTo("hibernate");
		assertThat(hibernate.getValue()).isEqualTo(hibernateCount);

		KeyValue<String, Long> java = iterator.next();
		assertThat(java.getKey()).isEqualTo("java");
		assertThat(java.getValue()).isEqualTo(javaCount);

		KeyValue<String, Long> scala = iterator.next();
		assertThat(scala.getKey()).isEqualTo("scala");
		assertThat(scala.getValue()).isEqualTo(scalaCount);
	}

	@Test
	public void should_exception_on_remove_counter_widemap() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Long> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", 44654L);

		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");

		popularTopics.remove("cassandra");

		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot remove counter value. Please set a its value to 0 instead of removing it");

		popularTopics.remove("cassandra", null, BoundingMode.INCLUSIVE_BOUNDS);
	}

	@Test
	public void should_remove_counter_widemap_when_entity_remove() throws Exception
	{
		long javaCount = 4564, cassandraCount = 873, scalaCount = 321, groovyCount = 54364, //
		hibernateCount = 545365464, springCount = 845143654;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Long> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", cassandraCount);
		popularTopics.insert("groovy", groovyCount);
		popularTopics.insert("hibernate", hibernateCount);
		popularTopics.insert("java", javaCount);
		popularTopics.insert("scala", scalaCount);
		popularTopics.insert("spring", springCount);

		em.remove(bean);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		DynamicComposite comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics",
				"cassandra");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics", "groovy");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics", "hibernate");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics", "java");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics", "scala");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createCounterName(EXTERNAL_WIDE_MAP_COUNTER, "popularTopics", "spring");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

	}

	private Tweet createTweet()
	{
		Tweet tweet = new Tweet();
		tweet.setId(TimeUUIDUtils.getUniqueTimeUUIDinMillis());
		tweet.setContent("welcome!");
		return tweet;
	}

	private <T> Composite createCounterKey(Class<T> clazz, Long id)
	{
		Composite comp = new Composite();
		comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
		comp.setComponent(1, id.toString(), STRING_SRZ);
		return comp;
	}

	private <T> Composite createCounterKey(Class<T> clazz, UUID id) throws Exception
	{
		Composite comp = new Composite();
		comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
		comp.setComponent(1, mapper.writeValueAsString(id), STRING_SRZ);
		return comp;
	}

	private DynamicComposite createCounterName(PropertyType type, String propertyName)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		return composite;
	}

	private DynamicComposite createCounterName(PropertyType type, String propertyName, String key)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		composite.addComponent(2, key, ComponentEquality.EQUAL);
		return composite;
	}
}
