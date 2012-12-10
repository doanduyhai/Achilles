package fr.doan.achilles.entity.manager;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getDao;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.normalizeColumnFamilyName;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import mapping.entity.CompleteBean;
import mapping.entity.CompleteBean.UserTweetKey;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.type.KeyValue;
import fr.doan.achilles.entity.type.WideMap;

/**
 * ThriftEntityManagerMultiKeyWideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityManagerMultiKeyWideMapIT
{
	private final String ENTITY_PACKAGE = "mapping.entity";
	private GenericDao<Long> dao = getDao(LONG_SRZ,
			normalizeColumnFamilyName(CompleteBean.class.getCanonicalName()));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

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

		userTweets.insertValue(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insertValue(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insertValue(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insertValue(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insertValue(new UserTweetKey(qux, uuid5), "tweet5-qux");

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, bar, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, qux, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(),
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
		userTweets.insertValue(new UserTweetKey(bar, uuid1), "tweet1-bar", 150);

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, bar, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, bar, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<DynamicComposite, Object>> columns = dao.findRawColumnsRange(bean.getId(),
				startComp, endComp, false, 10);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(150);

	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		UserTweetKey userTweetKey = new UserTweetKey(bar, uuid1);
		userTweets.insertValue(userTweetKey, "tweet1-bar");

		assertThat(userTweets.getValue(userTweetKey)).isEqualTo("tweet1-bar");
	}

	@Test
	public void should_find_values_by_range_exclusive_start_inclusive_end_reverse()
			throws Exception
	{

		userTweets.insertValue(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insertValue(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insertValue(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insertValue(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insertValue(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.findValues( //
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
		userTweets.insertValue(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insertValue(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insertValue(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insertValue(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insertValue(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.findValues( //
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
		userTweets.insertValue(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insertValue(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insertValue(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insertValue(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insertValue(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.findValues( //
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
		userTweets.insertValue(new UserTweetKey(bar, uuid1), "tweet1-bar");
		userTweets.insertValue(new UserTweetKey(bar, uuid2), "tweet2-bar");
		userTweets.insertValue(new UserTweetKey(foo, uuid3), "tweet3-foo");
		userTweets.insertValue(new UserTweetKey(qux, uuid4), "tweet4-qux");
		userTweets.insertValue(new UserTweetKey(qux, uuid5), "tweet5-qux");

		List<KeyValue<UserTweetKey, String>> results = userTweets.findValues( //
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

	private DynamicComposite buildComposite()
	{
		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "userTweets", ComponentEquality.EQUAL);
		return startComp;
	}
}
