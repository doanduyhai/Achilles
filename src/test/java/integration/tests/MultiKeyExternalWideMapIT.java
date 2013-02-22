package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyBuilder.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getCompositeDao;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap;
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
public class MultiKeyExternalWideMapIT
{
	private GenericCompositeDao<Long, String> multiKeyExternalWideMapDao = getCompositeDao(
			LONG_SRZ, STRING_SRZ, normalizerAndValidateColumnFamilyName("MultiKeyExternalWideMap"));

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private CompleteBean bean;

	private WideMap<UserTweetKey, String> multiKeyExternalWideMap;

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
		multiKeyExternalWideMap = bean.getMultiKeyExternalWideMap();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar", 150);

		Composite startComp = new Composite();
		startComp.addComponent(0, bar, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, bar, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<Composite, String>> columns = multiKeyExternalWideMapDao.findRawColumnsRange(
				bean.getId(), startComp, endComp, false, 10);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(150);

	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		UserTweetKey userTweetKey = new UserTweetKey(bar, uuid1);
		multiKeyExternalWideMap.insert(userTweetKey, "tweet1-bar");

		assertThat(multiKeyExternalWideMap.get(userTweetKey)).isEqualTo("tweet1-bar");
	}

	@Test
	public void should_find_values_by_range_exclusive_start_inclusive_end_reverse()
			throws Exception
	{

		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = multiKeyExternalWideMap.find( //
				new UserTweetKey(qux, uuid5), false, //
				new UserTweetKey(foo, uuid3), true, //
				true, 10);

		assertThat(results).hasSize(2);

		assertThat(results.get(0).getKey().getTweet()).isEqualTo(uuid4);
		assertThat(results.get(0).getKey().getUser()).isEqualTo(qux);

		assertThat(results.get(1).getKey().getTweet()).isEqualTo(uuid3);
		assertThat(results.get(1).getKey().getUser()).isEqualTo(foo);

	}

	@Test
	public void should_find_values_by_asc_range_with_start_having_null() throws Exception
	{
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = multiKeyExternalWideMap.find( //
				new UserTweetKey(bar, null), true, //
				new UserTweetKey(foo, uuid3), true, //
				false, 10);

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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = multiKeyExternalWideMap.find( //
				new UserTweetKey(bar, uuid1), true, //
				new UserTweetKey(foo, null), true, //
				false, 10);

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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = multiKeyExternalWideMap.find( //
				null, true, //
				new UserTweetKey(foo, null), true, //
				false, 10);

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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		KeyValueIterator<UserTweetKey, String> iter = multiKeyExternalWideMap.iterator( //
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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		KeyValueIterator<UserTweetKey, String> iter = //
		multiKeyExternalWideMap.iterator( //
				new UserTweetKey(qux, uuid5), false, //
				new UserTweetKey(bar, uuid1), true, //
				true, 2);

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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		multiKeyExternalWideMap.remove(new UserTweetKey(bar, uuid2));

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
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid1), "tweet1-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(bar, uuid2), "tweet2-bar");
		multiKeyExternalWideMap.insert(new UserTweetKey(foo, uuid3), "tweet3-foo");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid4), "tweet4-qux");
		multiKeyExternalWideMap.insert(new UserTweetKey(qux, uuid5), "tweet5-qux");

		multiKeyExternalWideMap.remove( //
				new UserTweetKey(bar, uuid2), true, //
				new UserTweetKey(qux, uuid4), false);

		List<Pair<Composite, String>> columns = multiKeyExternalWideMapDao.findColumnsRange(
				bean.getId(), null, null, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("tweet1-bar");
		assertThat(columns.get(1).right).isEqualTo("tweet4-qux");
		assertThat(columns.get(2).right).isEqualTo("tweet5-qux");
	}

}
