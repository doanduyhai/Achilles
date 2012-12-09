package fr.doan.achilles.entity.manager;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getDao;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.normalizeColumnFamilyName;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.WideMap;

public class ThriftEntityManagerWideMapIT
{

	private final String ENTITY_PACKAGE = "mapping.entity";
	private GenericDao<Long> dao = getDao(LONG_SRZ,
			normalizeColumnFamilyName(CompleteBean.class.getCanonicalName()));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private CompleteBean bean;

	private WideMap<UUID, String> tweets;

	private UUID uuid1 = TimeUUIDUtils.getTimeUUID(1);
	private UUID uuid2 = TimeUUIDUtils.getTimeUUID(2);
	private UUID uuid3 = TimeUUIDUtils.getTimeUUID(3);
	private UUID uuid4 = TimeUUIDUtils.getTimeUUID(4);
	private UUID uuid5 = TimeUUIDUtils.getTimeUUID(5);

	@Before
	public void setUp()
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
		bean = em.merge(bean);
		tweets = bean.getTweets();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		insert3Tweets();

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, uuid1, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, uuid3, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("tweet1");
		assertThat(columns.get(1).right).isEqualTo("tweet2");
		assertThat(columns.get(2).right).isEqualTo("tweet3");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		tweets.insertValue(uuid1, "tweet1", 15);
		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, uuid1, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, uuid2, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<DynamicComposite, Object>> columns = dao.findRawColumnsRange(bean.getId(),
				startComp, endComp, false, 10);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(15);
	}

	@Test
	public void should_not_find_value_after_ttl_expiration() throws Exception
	{
		tweets.insertValue(uuid1, "tweet1", 1);

		Thread.sleep(1001);

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, uuid1, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, uuid2, ComponentEquality.GREATER_THAN_EQUAL);
		List<HColumn<DynamicComposite, Object>> columns = dao.findRawColumnsRange(bean.getId(),
				startComp, endComp, false, 10);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		insert3Tweets();

		assertThat(tweets.getValue(uuid1)).isEqualTo("tweet1");
	}

	@Test
	public void should_find_values_by_range() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(uuid1, uuid5, false, 10);

		assertThat(foundTweets).hasSize(5);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("tweet3");
		assertThat(foundTweets.get(3).getValue()).isEqualTo("tweet4");
		assertThat(foundTweets.get(4).getValue()).isEqualTo("tweet5");

	}

	@Test
	public void should_find_values_by_range_with_limit() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(uuid2, uuid5, false, 3);

		assertThat(foundTweets).hasSize(3);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet2");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet3");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("tweet4");

	}

	@Test
	public void should_find_values_by_range_with_exclusive_range() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundTweets = tweets
				.findValues(uuid2, uuid5, false, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet3");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet4");
	}

	@Test
	public void should_find_values_by_range_with_exclusive_start_inclusive_end() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(uuid2, false, uuid4, true,
				false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet3");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet4");
	}

	@Test
	public void should_find_values_by_range_with_null_start() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(null, uuid2, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet2");
	}

	@Test
	public void should_find_values_by_range_with_null_start_and_end() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(null, null, false, 10);

		assertThat(foundTweets).hasSize(5);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("tweet3");
		assertThat(foundTweets.get(3).getValue()).isEqualTo("tweet4");
		assertThat(foundTweets.get(4).getValue()).isEqualTo("tweet5");
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Tweets();

		Iterator<KeyValue<UUID, String>> iter = tweets.iterator(null, null, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("tweet1");
		assertThat(iter.next().getValue()).isEqualTo("tweet2");
		assertThat(iter.next().getValue()).isEqualTo("tweet3");
		assertThat(iter.next().getValue()).isEqualTo("tweet4");
		assertThat(iter.next().getValue()).isEqualTo("tweet5");

	}

	@Test
	public void should_get_iterator_exclusive_bounds() throws Exception
	{
		insert5Tweets();

		Iterator<KeyValue<UUID, String>> iter = tweets.iterator(uuid2, uuid4, false, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("tweet3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_get_iterator_inclusive_start_exclusive_end() throws Exception
	{
		insert5Tweets();

		Iterator<KeyValue<UUID, String>> iter = tweets.iterator(uuid2, true, uuid4, false, false,
				10);

		assertThat(iter.next().getValue()).isEqualTo("tweet2");
		assertThat(iter.next().getValue()).isEqualTo("tweet3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Tweets();

		tweets.removeValue(uuid1);

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(null, null, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet2");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet3");
	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Tweets();

		tweets.removeValues(uuid2, uuid4);

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(null, null, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet5");
	}

	@Test
	public void should_remove_values_range_exclusive_bounds() throws Exception
	{
		insert5Tweets();

		tweets.removeValues(uuid2, uuid5, false);

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(null, null, false, 10);

		assertThat(foundTweets).hasSize(3);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("tweet5");
	}

	@Test
	public void should_remove_values_range_inclusive_start_exclusive_end() throws Exception
	{
		insert5Tweets();

		tweets.removeValues(uuid2, true, uuid5, false);

		List<KeyValue<UUID, String>> foundTweets = tweets.findValues(null, null, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("tweet5");
	}

	private DynamicComposite buildComposite()
	{
		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "tweets", ComponentEquality.EQUAL);
		return startComp;
	}

	private void insert3Tweets()
	{
		tweets.insertValue(uuid1, "tweet1");
		tweets.insertValue(uuid2, "tweet2");
		tweets.insertValue(uuid3, "tweet3");
	}

	private void insert5Tweets()
	{
		tweets.insertValue(uuid1, "tweet1");
		tweets.insertValue(uuid2, "tweet2");
		tweets.insertValue(uuid3, "tweet3");
		tweets.insertValue(uuid4, "tweet4");
		tweets.insertValue(uuid5, "tweet5");
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
