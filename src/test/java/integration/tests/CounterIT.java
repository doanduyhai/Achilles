package integration.tests;

import static info.archinnov.achilles.entity.type.WideMap.BoundingMode.*;
import static info.archinnov.achilles.entity.type.WideMap.OrderingMode.ASCENDING;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.serializer.SerializerUtils;
import info.archinnov.achilles.wrapper.CounterBuilder;
import integration.tests.entity.BeanWithConsistencyLevelOnClassAndField;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.math.RandomUtils;
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

	private CounterDao counterDao = ThriftCassandraDaoTest.getCounterDao();
	private GenericWideRowDao<Long, Long> popularTopicsDao = ThriftCassandraDaoTest
			.getColumnFamilyDao(LONG_SRZ, LONG_SRZ, "complete_bean_popular_topics");
	private GenericWideRowDao<Long, Long> counterWideMapDao = ThriftCassandraDaoTest
			.getColumnFamilyDao(LONG_SRZ, LONG_SRZ, "counter_widemap");

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();
	private CompleteBean bean;

	@Before
	public void setUp()
	{
		counterDao.truncateCounters();
	}

	@Test
	public void should_persist_counter() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = em.merge(bean);
		bean.getVersion().incr(2L);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		Composite comp = createCounterName("version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

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
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(version);

		// Pause required to let Cassandra remove counter columns
		Thread.sleep(1000);

		em.remove(bean);

		actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(0);
	}

	@Test
	public void should_insert_counter_widemap() throws Exception
	{
		long javaCount = 1234, cassandraCount = 567;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = em.merge(bean);

		bean.getPopularTopics().insert("cassandra", CounterBuilder.incr(cassandraCount));
		bean.getPopularTopics().insert("java", CounterBuilder.incr(javaCount));

		Composite comp = createWideMapCounterName("java");

		assertThat(popularTopicsDao.getCounterValue(bean.getId(), comp)).isEqualTo(javaCount);

		comp = createWideMapCounterName("cassandra");

		assertThat(popularTopicsDao.getCounterValue(bean.getId(), comp)).isEqualTo(cassandraCount);
	}

	@Test
	public void should_find_counter_widemap() throws Exception
	{
		long javaCount = 655, cassandraCount = 5464, scalaCount = 1231;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Counter> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", CounterBuilder.incr(cassandraCount));
		popularTopics.insert("java", CounterBuilder.incr(javaCount));
		popularTopics.insert("scala", CounterBuilder.incr(scalaCount));

		List<KeyValue<String, Counter>> foundKeyValues = popularTopics.find("cassandra", null, 10,
				INCLUSIVE_BOUNDS, ASCENDING);

		assertThat(foundKeyValues).hasSize(3);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo("cassandra");
		assertThat(foundKeyValues.get(0).getValue().get()).isEqualTo(cassandraCount);
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo("java");
		assertThat(foundKeyValues.get(1).getValue().get()).isEqualTo(javaCount);
		assertThat(foundKeyValues.get(2).getKey()).isEqualTo("scala");
		assertThat(foundKeyValues.get(2).getValue().get()).isEqualTo(scalaCount);

		List<String> foundKeys = popularTopics.findKeys("cassandra", null, 10, INCLUSIVE_BOUNDS,
				ASCENDING);

		assertThat(foundKeys).hasSize(3);
		assertThat(foundKeys.get(0)).isEqualTo("cassandra");
		assertThat(foundKeys.get(1)).isEqualTo("java");
		assertThat(foundKeys.get(2)).isEqualTo("scala");

		List<Counter> foundValues = popularTopics.findValues("cassandra", null, 10,
				INCLUSIVE_BOUNDS, ASCENDING);

		assertThat(foundValues).hasSize(3);
		assertThat(foundValues.get(0).get()).isEqualTo(cassandraCount);
		assertThat(foundValues.get(1).get()).isEqualTo(javaCount);
		assertThat(foundValues.get(2).get()).isEqualTo(scalaCount);
	}

	@Test
	public void should_iterate_on_counter_widemap() throws Exception
	{
		long javaCount = 4564, cassandraCount = 873, scalaCount = 321, groovyCount = 54364, //
		hibernateCount = 545365464, springCount = 845143654;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Counter> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", CounterBuilder.incr(cassandraCount));
		popularTopics.insert("groovy", CounterBuilder.incr(groovyCount));
		popularTopics.insert("hibernate", CounterBuilder.incr(hibernateCount));
		popularTopics.insert("java", CounterBuilder.incr(javaCount));
		popularTopics.insert("scala", CounterBuilder.incr(scalaCount));
		popularTopics.insert("spring", CounterBuilder.incr(springCount));

		Iterator<KeyValue<String, Counter>> iterator = popularTopics.iterator("groovy", "spring",
				2, INCLUSIVE_START_BOUND_ONLY, ASCENDING);

		KeyValue<String, Counter> groovy = iterator.next();
		assertThat(groovy.getKey()).isEqualTo("groovy");
		assertThat(groovy.getValue().get()).isEqualTo(groovyCount);

		KeyValue<String, Counter> hibernate = iterator.next();
		assertThat(hibernate.getKey()).isEqualTo("hibernate");
		assertThat(hibernate.getValue().get()).isEqualTo(hibernateCount);

		KeyValue<String, Counter> java = iterator.next();
		assertThat(java.getKey()).isEqualTo("java");
		assertThat(java.getValue().get()).isEqualTo(javaCount);

		KeyValue<String, Counter> scala = iterator.next();
		assertThat(scala.getKey()).isEqualTo("scala");
		assertThat(scala.getValue().get()).isEqualTo(scalaCount);
	}

	@Test
	public void should_exception_on_remove_counter_widemap() throws Exception
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Counter> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", CounterBuilder.incr(44654L));

		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("Cannot remove counter value");

		popularTopics.remove("cassandra");

		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("Cannot remove counter value");

		popularTopics.remove("cassandra", null, BoundingMode.INCLUSIVE_BOUNDS);
	}

	@Test
	public void should_remove_counter_widemap_when_entity_remove() throws Exception
	{
		long javaCount = 4564, cassandraCount = 873, scalaCount = 321, groovyCount = 54364, //
		hibernateCount = 545365464, springCount = 845143654;

		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = em.merge(bean);

		WideMap<String, Counter> popularTopics = bean.getPopularTopics();

		popularTopics.insert("cassandra", CounterBuilder.incr(cassandraCount));
		popularTopics.insert("groovy", CounterBuilder.incr(groovyCount));
		popularTopics.insert("hibernate", CounterBuilder.incr(hibernateCount));
		popularTopics.insert("java", CounterBuilder.incr(javaCount));
		popularTopics.insert("scala", CounterBuilder.incr(scalaCount));
		popularTopics.insert("spring", CounterBuilder.incr(springCount));

		// Pause required to let Cassandra remove counter columns
		Thread.sleep(1000);

		em.remove(bean);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		Composite comp = createWideMapCounterName("cassandra");

		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createWideMapCounterName("groovy");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createWideMapCounterName("hibernate");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createWideMapCounterName("java");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createWideMapCounterName("scala");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

		comp = createWideMapCounterName("spring");
		assertThat(counterDao.getCounterValue(keyComp, comp)).isEqualTo(0L);

	}

	@Test
	public void should_incr_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		counterWideMap.insert(10, CounterBuilder.incr());
		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(1L);
	}

	@Test
	public void should_incr_n_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		counterWideMap.insert(10, CounterBuilder.incr(15L));
		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(15L);
	}

	@Test
	public void should_decr_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		counterWideMap.insert(10, CounterBuilder.decr());
		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(-1L);
	}

	@Test
	public void should_decr_n_for_counter_widemap() throws Exception
	{
		BeanWithConsistencyLevelOnClassAndField entity = prepareCounterWideMap();
		WideMap<Integer, Counter> counterWideMap = entity.getCounterWideMap();

		counterWideMap.insert(10, CounterBuilder.decr(15L));
		assertThat(counterWideMapDao.getCounterValue(entity.getId(), prepareCounterWideMapName(10)))
				.isEqualTo(-15L);
	}

	private BeanWithConsistencyLevelOnClassAndField prepareCounterWideMap()
	{
		BeanWithConsistencyLevelOnClassAndField entity = new BeanWithConsistencyLevelOnClassAndField();
		entity.setId(RandomUtils.nextLong());
		entity.setName("name");
		entity = em.merge(entity);

		return entity;
	}

	private Composite prepareCounterWideMapName(Integer index)
	{
		Composite comp = new Composite();
		comp.addComponent(10, SerializerUtils.INT_SRZ);
		return comp;
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

	private Composite createWideMapCounterName(String key)
	{
		Composite composite = new Composite();
		composite.addComponent(0, key, ComponentEquality.EQUAL);
		return composite;
	}
}
