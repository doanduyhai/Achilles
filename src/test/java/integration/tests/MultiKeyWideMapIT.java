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
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBean.UserTweetKey;
import integration.tests.entity.CompleteBeanTestBuilder;

import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;

/**
 * MultiKeyWideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapIT
{
	private GenericDynamicCompositeDao<Long> dao = getDynamicCompositeDao(LONG_SRZ,
			normalizerAndValidateColumnFamilyName(CompleteBean.class.getName()));

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private CompleteBean bean;

	private WideMap<UserTweetKey, String> userTweets;

	private UUID uuid1 = TimeUUIDUtils.getTimeUUID(1);
	private UUID uuid2 = TimeUUIDUtils.getTimeUUID(2);
	private UUID uuid3 = TimeUUIDUtils.getTimeUUID(3);
	private UUID uuid4 = TimeUUIDUtils.getTimeUUID(4);
	private UUID uuid5 = TimeUUIDUtils.getTimeUUID(5);

	private String foo = "foo";
	private String bar = "bar";
	private String qux = "qux";

	@Before
	public void setUp()
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
		bean = em.merge(bean);
		userTweets = bean.getUserTweets();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, bar, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, qux, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(5);
		assertThat(columns.get(0).right).isEqualTo("tweet1-bar");
		assertThat(columns.get(1).right).isEqualTo("tweet2-bar");
		assertThat(columns.get(2).right).isEqualTo("tweet3-foo");
		assertThat(columns.get(3).right).isEqualTo("tweet4-qux");
		assertThat(columns.get(4).right).isEqualTo("tweet5-qux");
	}

	@Test
	public void should_insert_values_with_ttl() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar", 150);

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, bar, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, bar, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<DynamicComposite, String>> columns = dao.findRawColumnsRange(bean.getId(),
				startComp, endComp, 10, false);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(150);

	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		UserTweetKey userTweetKey = new UserTweetKey(bar, uuid1);
		userTweets.insert(userTweetKey, "tweet1-bar");

		assertThat(userTweets.get(userTweetKey)).isEqualTo("tweet1-bar");
	}

	@Test
	public void should_find_values_by_range_exclusive_start_inclusive_end_reverse()
			throws Exception
	{

		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.find(new UserTweetKey(qux, uuid5), new UserTweetKey(foo, uuid3), 10, 
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(results).hasSize(2);

		assertThat(results.get(0).getKey().getTweet()).isEqualTo(uuid4);
		assertThat(results.get(0).getKey().getUser()).isEqualTo(qux);

		assertThat(results.get(1).getKey().getTweet()).isEqualTo(uuid3);
		assertThat(results.get(1).getKey().getUser()).isEqualTo(foo);

	}

	@Test
	public void should_find_values_by_asc_range_with_start_having_null() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.find(new UserTweetKey(bar, null), new UserTweetKey(foo, uuid3), 
				10, BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);

		assertThat(results).hasSize(3);

		assertThat(results.get(0).getKey().getTweet()).isEqualTo(uuid1);
		assertThat(results.get(0).getKey().getUser()).isEqualTo(bar);

		assertThat(results.get(1).getKey().getTweet()).isEqualTo(uuid2);
		assertThat(results.get(1).getKey().getUser()).isEqualTo(bar);

		assertThat(results.get(2).getKey().getTweet()).isEqualTo(uuid3);
		assertThat(results.get(2).getKey().getUser()).isEqualTo(foo);
	}

	@Test
	public void should_find_values_by_asc_range_with_end_having_null() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.find(new UserTweetKey(bar, uuid1), new UserTweetKey(foo, null), 10, 
				BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);

		assertThat(results).hasSize(3);

		assertThat(results.get(0).getKey().getTweet()).isEqualTo(uuid1);
		assertThat(results.get(0).getKey().getUser()).isEqualTo(bar);

		assertThat(results.get(1).getKey().getTweet()).isEqualTo(uuid2);
		assertThat(results.get(1).getKey().getUser()).isEqualTo(bar);

		assertThat(results.get(2).getKey().getTweet()).isEqualTo(uuid3);
		assertThat(results.get(2).getKey().getUser()).isEqualTo(foo);
	}

	@Test
	public void should_find_values_by_asc_range_with_start_completely_null() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.find(null, new UserTweetKey(foo, null), 10, 
				BoundingMode.INCLUSIVE_BOUNDS, OrderingMode.ASCENDING);

		assertThat(results).hasSize(3);

		assertThat(results.get(0).getKey().getTweet()).isEqualTo(uuid1);
		assertThat(results.get(0).getKey().getUser()).isEqualTo(bar);

		assertThat(results.get(1).getKey().getTweet()).isEqualTo(uuid2);
		assertThat(results.get(1).getKey().getUser()).isEqualTo(bar);

		assertThat(results.get(2).getKey().getTweet()).isEqualTo(uuid3);
		assertThat(results.get(2).getKey().getUser()).isEqualTo(foo);
	}

	@Test
	public void should_iterate() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		KeyValueIterator<UserTweetKey, String> iter = userTweets.iterator( //
				new UserTweetKey(foo, uuid3), //
				new UserTweetKey(qux, uuid5), //
				5);

		assertThat(iter.hasNext());
		KeyValue<UserTweetKey, String> keyValue1 = iter.next();
		KeyValue<UserTweetKey, String> keyValue2 = iter.next();
		KeyValue<UserTweetKey, String> keyValue3 = iter.next();

		assertThat(keyValue1.getKey().getUser()).isEqualTo(foo);
		assertThat(keyValue1.getKey().getTweet()).isEqualTo(uuid3);
		assertThat(keyValue1.getValue()).isEqualTo("tweet3-foo");

		assertThat(keyValue2.getKey().getUser()).isEqualTo(qux);
		assertThat(keyValue2.getKey().getTweet()).isEqualTo(uuid4);
		assertThat(keyValue2.getValue()).isEqualTo("tweet4-qux");

		assertThat(keyValue3.getKey().getUser()).isEqualTo(qux);
		assertThat(keyValue3.getKey().getTweet()).isEqualTo(uuid5);
		assertThat(keyValue3.getValue()).isEqualTo("tweet5-qux");
	}

	@Test
	public void should_iterate_desc_exclusive_start_inclusive_end_with_count() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		KeyValueIterator<UserTweetKey, String> iter = //
		userTweets.iterator(new UserTweetKey(qux, uuid5), new UserTweetKey(bar, uuid1), 2, BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(iter.hasNext());

		KeyValue<UserTweetKey, String> keyValue1 = iter.next();
		KeyValue<UserTweetKey, String> keyValue2 = iter.next();

		assertThat(keyValue1.getKey().getUser()).isEqualTo(qux);
		assertThat(keyValue1.getKey().getTweet()).isEqualTo(uuid4);
		assertThat(keyValue1.getValue()).isEqualTo("tweet4-qux");

		assertThat(keyValue2.getKey().getUser()).isEqualTo(foo);
		assertThat(keyValue2.getKey().getTweet()).isEqualTo(uuid3);
		assertThat(keyValue2.getValue()).isEqualTo("tweet3-foo");

	}

	@Test
	public void should_remove() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		userTweets.remove(new UserTweetKey(bar, uuid2));

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, bar, ComponentEquality.EQUAL);
		startComp.addComponent(3, uuid2, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, bar, ComponentEquality.EQUAL);
		endComp.addComponent(3, uuid2, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_remove_inclusive_start_exclusive_end() throws Exception
	{
		userTweets.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		userTweets.remove(new UserTweetKey(bar, uuid2), new UserTweetKey(qux, uuid4), BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "userTweets", ComponentEquality.EQUAL);

		DynamicComposite endComp = new DynamicComposite();
		endComp.addComponent(0, WIDE_MAP.flag(), ComponentEquality.EQUAL);
		endComp.addComponent(1, "userTweets", ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, String>> columns = dao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("tweet1-bar");
		assertThat(columns.get(1).right).isEqualTo("tweet4-qux");
		assertThat(columns.get(2).right).isEqualTo("tweet5-qux");
	}

	private DynamicComposite buildComposite()
	{
		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "userTweets", ComponentEquality.EQUAL);
		return startComp;
	}
}
