package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getDynamicCompositeDao;
import static info.archinnov.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * WideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapIT
{

	private GenericDynamicCompositeDao<Long> dao = getDynamicCompositeDao(LONG_SRZ,
			normalizerAndValidateColumnFamilyName(CompleteBean.class.getName()));

	private ThriftEntityManager em = CassandraDaoTest.getEm();

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

		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("tweet1");
		assertThat(columns.get(1).right).isEqualTo("tweet2");
		assertThat(columns.get(2).right).isEqualTo("tweet3");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		tweets.insert(uuid1, "tweet1", 15);
		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, uuid1, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, uuid2, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<DynamicComposite, String>> columns = dao.findRawColumnsRange(bean.getId(),
				startComp, endComp, 10, false);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(15);
	}

	@Test
	public void should_not_find_value_after_ttl_expiration() throws Exception
	{
		tweets.insert(uuid1, "tweet1", 1);

		Thread.sleep(1001);

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, uuid1, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, uuid2, ComponentEquality.GREATER_THAN_EQUAL);
		List<HColumn<DynamicComposite, String>> columns = dao.findRawColumnsRange(bean.getId(),
				startComp, endComp, 10, false);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		insert3Tweets();

		assertThat(tweets.get(uuid1)).isEqualTo("tweet1");
	}

	@Test
	public void should_find_by_range() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(uuid1, uuid5, 10);

		assertThat(foundKeyValues).hasSize(5);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo(uuid1);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo(uuid2);
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet2");
		assertThat(foundKeyValues.get(2).getKey()).isEqualTo(uuid3);
		assertThat(foundKeyValues.get(2).getValue()).isEqualTo("tweet3");
		assertThat(foundKeyValues.get(3).getKey()).isEqualTo(uuid4);
		assertThat(foundKeyValues.get(3).getValue()).isEqualTo("tweet4");
		assertThat(foundKeyValues.get(4).getKey()).isEqualTo(uuid5);
		assertThat(foundKeyValues.get(4).getValue()).isEqualTo("tweet5");

		List<String> foundValues = tweets.findValues(uuid1, uuid5, 10);

		assertThat(foundValues.get(0)).isEqualTo("tweet1");
		assertThat(foundValues.get(1)).isEqualTo("tweet2");
		assertThat(foundValues.get(2)).isEqualTo("tweet3");
		assertThat(foundValues.get(3)).isEqualTo("tweet4");
		assertThat(foundValues.get(4)).isEqualTo("tweet5");

		List<UUID> foundKeys = tweets.findKeys(uuid1, uuid5, 10);

		assertThat(foundKeys.get(0)).isEqualTo(uuid1);
		assertThat(foundKeys.get(1)).isEqualTo(uuid2);
		assertThat(foundKeys.get(2)).isEqualTo(uuid3);
		assertThat(foundKeys.get(3)).isEqualTo(uuid4);
		assertThat(foundKeys.get(4)).isEqualTo(uuid5);
	}

	@Test
	public void should_find_values_by_range_with_limit() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(uuid2, uuid5, 3);

		assertThat(foundKeyValues).hasSize(3);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo(uuid2);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet2");
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo(uuid3);
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet3");
		assertThat(foundKeyValues.get(2).getKey()).isEqualTo(uuid4);
		assertThat(foundKeyValues.get(2).getValue()).isEqualTo("tweet4");

		List<String> foundValues = tweets.findValues(uuid2, uuid5, 3);

		assertThat(foundValues.get(0)).isEqualTo("tweet2");
		assertThat(foundValues.get(1)).isEqualTo("tweet3");
		assertThat(foundValues.get(2)).isEqualTo("tweet4");

		List<UUID> foundKeys = tweets.findKeys(uuid2, uuid5, 3);

		assertThat(foundKeys.get(0)).isEqualTo(uuid2);
		assertThat(foundKeys.get(1)).isEqualTo(uuid3);
		assertThat(foundKeys.get(2)).isEqualTo(uuid4);

	}

	@Test
	public void should_find_values_by_range_with_exclusive_range() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundKeyValues = tweets.findBoundsExclusive(uuid2, uuid5, 10);

		assertThat(foundKeyValues).hasSize(2);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo(uuid3);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet3");
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo(uuid4);
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet4");

		List<String> foundValues = tweets.findBoundsExclusiveValues(uuid2, uuid5, 10);

		assertThat(foundValues.get(0)).isEqualTo("tweet3");
		assertThat(foundValues.get(1)).isEqualTo("tweet4");

		List<UUID> foundKeys = tweets.findBoundsExclusiveKeys(uuid2, uuid5, 10);

		assertThat(foundKeys.get(0)).isEqualTo(uuid3);
		assertThat(foundKeys.get(1)).isEqualTo(uuid4);
	}

	@Test
	public void should_find_values_by_range_with_exclusive_start_inclusive_end_reverse()
			throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(uuid4, uuid2, 10, 
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(foundKeyValues).hasSize(2);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo(uuid3);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet3");
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo(uuid2);
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet2");

		List<String> foundValues = tweets.findValues(uuid4, uuid2, 10, 
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(foundValues.get(0)).isEqualTo("tweet3");
		assertThat(foundValues.get(1)).isEqualTo("tweet2");

		List<UUID> foundKeys = tweets.findKeys(uuid4, uuid2, 10, 
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(foundKeys.get(0)).isEqualTo(uuid3);
		assertThat(foundKeys.get(1)).isEqualTo(uuid2);
	}

	@Test
	public void should_find_values_by_range_with_null_start() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(null, uuid2, 10);

		assertThat(foundKeyValues).hasSize(2);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo(uuid1);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo(uuid2);
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet2");

		List<String> foundValues = tweets.findValues(null, uuid2, 10);

		assertThat(foundValues.get(0)).isEqualTo("tweet1");
		assertThat(foundValues.get(1)).isEqualTo("tweet2");

		List<UUID> foundKeys = tweets.findKeys(null, uuid2, 10);

		assertThat(foundKeys.get(0)).isEqualTo(uuid1);
		assertThat(foundKeys.get(1)).isEqualTo(uuid2);
	}

	@Test
	public void should_find_values_by_range_with_null_start_and_end() throws Exception
	{
		insert5Tweets();

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(null, null, 10);

		assertThat(foundKeyValues).hasSize(5);
		assertThat(foundKeyValues.get(0).getKey()).isEqualTo(uuid1);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundKeyValues.get(1).getKey()).isEqualTo(uuid2);
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet2");
		assertThat(foundKeyValues.get(2).getKey()).isEqualTo(uuid3);
		assertThat(foundKeyValues.get(2).getValue()).isEqualTo("tweet3");
		assertThat(foundKeyValues.get(3).getKey()).isEqualTo(uuid4);
		assertThat(foundKeyValues.get(3).getValue()).isEqualTo("tweet4");
		assertThat(foundKeyValues.get(4).getKey()).isEqualTo(uuid5);
		assertThat(foundKeyValues.get(4).getValue()).isEqualTo("tweet5");

		List<String> foundValues = tweets.findValues(null, null, 10);

		assertThat(foundValues.get(0)).isEqualTo("tweet1");
		assertThat(foundValues.get(1)).isEqualTo("tweet2");
		assertThat(foundValues.get(2)).isEqualTo("tweet3");
		assertThat(foundValues.get(3)).isEqualTo("tweet4");
		assertThat(foundValues.get(4)).isEqualTo("tweet5");

		List<UUID> foundKeys = tweets.findKeys(null, null, 10);

		assertThat(foundKeys.get(0)).isEqualTo(uuid1);
		assertThat(foundKeys.get(1)).isEqualTo(uuid2);
		assertThat(foundKeys.get(2)).isEqualTo(uuid3);
		assertThat(foundKeys.get(3)).isEqualTo(uuid4);
		assertThat(foundKeys.get(4)).isEqualTo(uuid5);

	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Tweets();

		Iterator<KeyValue<UUID, String>> iter = tweets.iterator(null, null, 10);

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

		Iterator<KeyValue<UUID, String>> iter = tweets.iteratorBoundsExclusive(uuid2, uuid4, 10);

		assertThat(iter.next().getValue()).isEqualTo("tweet3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_get_iterator_inclusive_start_exclusive_end() throws Exception
	{
		insert5Tweets();

		Iterator<KeyValue<UUID, String>> iter = tweets.iterator(uuid2, uuid4, 10, 
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);

		assertThat(iter.next().getValue()).isEqualTo("tweet2");
		assertThat(iter.next().getValue()).isEqualTo("tweet3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Tweets();

		tweets.remove(uuid1);

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(null, null, 10);

		assertThat(foundKeyValues).hasSize(2);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet2");
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet3");
	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Tweets();

		tweets.remove(uuid2, uuid4);

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(null, null, 10);

		assertThat(foundKeyValues).hasSize(2);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet5");
	}

	@Test
	public void should_remove_values_range_exclusive_bounds() throws Exception
	{
		insert5Tweets();

		tweets.removeBoundsExclusive(uuid2, uuid5);

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(null, null, 10);

		assertThat(foundKeyValues).hasSize(3);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet2");
		assertThat(foundKeyValues.get(2).getValue()).isEqualTo("tweet5");
	}

	@Test
	public void should_remove_values_range_inclusive_start_exclusive_end() throws Exception
	{
		insert5Tweets();

		tweets.remove(uuid2, uuid5, BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		List<KeyValue<UUID, String>> foundKeyValues = tweets.find(null, null, 10);

		assertThat(foundKeyValues).hasSize(2);
		assertThat(foundKeyValues.get(0).getValue()).isEqualTo("tweet1");
		assertThat(foundKeyValues.get(1).getValue()).isEqualTo("tweet5");
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
		tweets.insert(uuid1, "tweet1");
		tweets.insert(uuid2, "tweet2");
		tweets.insert(uuid3, "tweet3");
	}

	private void insert5Tweets()
	{
		tweets.insert(uuid1, "tweet1");
		tweets.insert(uuid2, "tweet2");
		tweets.insert(uuid3, "tweet3");
		tweets.insert(uuid4, "tweet4");
		tweets.insert(uuid5, "tweet5");
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
