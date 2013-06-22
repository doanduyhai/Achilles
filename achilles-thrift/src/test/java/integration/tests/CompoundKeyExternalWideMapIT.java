package integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getColumnFamilyDao;
import static info.archinnov.achilles.table.TableHelper.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.KeyValueIterator;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;
import info.archinnov.achilles.type.WideMap.BoundingMode;
import info.archinnov.achilles.type.WideMap.OrderingMode;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBean.UserTweetKey;
import integration.tests.entity.CompleteBeanTestBuilder;

import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.junit.Before;
import org.junit.Test;

/**
 * MultiKeyExternalWideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompoundKeyExternalWideMapIT
{
	private ThriftGenericWideRowDao multiKeyExternalWideMapDao = getColumnFamilyDao(
			normalizerAndValidateColumnFamilyName("complete_bean_multi_key_widemap"), Long.class,
			String.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private CompleteBean bean;

	private WideMap<UserTweetKey, String> compoundKeyExternalWideMap;

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
		compoundKeyExternalWideMap = bean.getMultiKeyWideMap();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		Composite startComp = new Composite();
		startComp.addComponent(0, bar, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, qux, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = multiKeyExternalWideMapDao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

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
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar", 150);

		Composite startComp = new Composite();
		startComp.addComponent(0, bar, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, bar, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<Composite, String>> columns = multiKeyExternalWideMapDao.findRawColumnsRange(
				bean.getId(), startComp, endComp, 10, false);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(150);

	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		UserTweetKey userTweetKey = new UserTweetKey(bar, uuid1);
		compoundKeyExternalWideMap.insert(userTweetKey, "tweet1-bar");

		assertThat(compoundKeyExternalWideMap.get(userTweetKey)).isEqualTo("tweet1-bar");
	}

	@Test
	public void should_find_values_by_range_exclusive_start_inclusive_end_reverse()
			throws Exception
	{

		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = compoundKeyExternalWideMap.find(
				//
				new UserTweetKey(qux, uuid5), new UserTweetKey(foo, uuid3), 10,
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
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = compoundKeyExternalWideMap.find(
				new UserTweetKey(bar, null), new UserTweetKey(foo, uuid3), 10,
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
	public void should_find_values_by_asc_range_with_end_having_null() throws Exception
	{
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = compoundKeyExternalWideMap.find(
				new UserTweetKey(bar, uuid1), new UserTweetKey(foo, null), 10,
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
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = compoundKeyExternalWideMap.find(null,
				new UserTweetKey(foo, null), 10, BoundingMode.INCLUSIVE_BOUNDS,
				OrderingMode.ASCENDING);

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
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		KeyValueIterator<UserTweetKey, String> iter = compoundKeyExternalWideMap.iterator( //
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
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		KeyValueIterator<UserTweetKey, String> iter = //
		compoundKeyExternalWideMap.iterator(new UserTweetKey(qux, uuid5),
				new UserTweetKey(bar, uuid1), 2, BoundingMode.INCLUSIVE_END_BOUND_ONLY,
				OrderingMode.DESCENDING);

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
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		compoundKeyExternalWideMap.remove(new UserTweetKey(bar, uuid2));

		Composite startComp = new Composite();
		startComp.addComponent(0, bar, ComponentEquality.EQUAL);
		startComp.addComponent(1, uuid2, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, bar, ComponentEquality.EQUAL);
		endComp.addComponent(1, uuid2, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = multiKeyExternalWideMapDao.findColumnsRange(
				bean.getId(), startComp, endComp, false, 20);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_remove_inclusive_start_exclusive_end() throws Exception
	{
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		compoundKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		compoundKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		compoundKeyExternalWideMap.remove(new UserTweetKey(bar, uuid2), new UserTweetKey(qux, uuid4),
				BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		List<Pair<Composite, String>> columns = multiKeyExternalWideMapDao.findColumnsRange(
				bean.getId(), null, null, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("tweet1-bar");
		assertThat(columns.get(1).right).isEqualTo("tweet4-qux");
		assertThat(columns.get(2).right).isEqualTo("tweet5-qux");
	}

}
