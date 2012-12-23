package integration.tests;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.normalizeColumnFamilyName;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;

import mapping.entity.MultiKeyWideRowBean;
import mapping.entity.WideRowMultiKey;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.common.CassandraDaoTest;
import fr.doan.achilles.dao.GenericWideRowDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.holder.KeyValue;

public class MultiKeyWideRowIT
{

	private final String ENTITY_PACKAGE = "mapping.entity";
	private GenericWideRowDao<Long, String> dao = CassandraDaoTest.getWideRowDao(LONG_SRZ,
			STRING_SRZ, normalizeColumnFamilyName(MultiKeyWideRowBean.class.getCanonicalName()));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private MultiKeyWideRowBean bean;

	private WideMap<WideRowMultiKey, String> map;

	private Long id = 452L;

	@Before
	public void setUp()
	{
		bean = new MultiKeyWideRowBean();
		bean.setId(id);
		bean = em.merge(bean);
		map = bean.getMap();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_insert_values() throws Exception
	{

		insert3Values();

		Composite startComp = new Composite();
		startComp.addComponent(0, 11L, EQUAL);
		startComp.addComponent(1, "11", EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 13L, EQUAL);
		endComp.addComponent(1, "13", GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), startComp,
				endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).left.getComponent(0).getValue(LONG_SRZ)).isEqualTo(11L);
		assertThat(columns.get(0).left.getComponent(1).getValue(STRING_SRZ)).isEqualTo("11");
		assertThat(columns.get(1).left.getComponent(0).getValue(LONG_SRZ)).isEqualTo(12L);
		assertThat(columns.get(1).left.getComponent(1).getValue(STRING_SRZ)).isEqualTo("12");
		assertThat(columns.get(2).left.getComponent(0).getValue(LONG_SRZ)).isEqualTo(13L);
		assertThat(columns.get(2).left.getComponent(1).getValue(STRING_SRZ)).isEqualTo("13");

	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		insert3Values();

		assertThat(map.get(new WideRowMultiKey(11L, "11"))).isEqualTo("value1");
	}

	@Test
	public void should_find_values_by_range() throws Exception
	{
		insert5Values();

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange( //
				new WideRowMultiKey(11L, "11"), //
				new WideRowMultiKey(15L, "15"), //
				false, 10);

		assertThat(foundTweets).hasSize(5);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(3).getValue()).isEqualTo("value4");
		assertThat(foundTweets.get(4).getValue()).isEqualTo("value5");

	}

	@Test
	public void should_find_values_by_range_with_limit() throws Exception
	{
		insert5Values();

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange( //
				new WideRowMultiKey(12L, "12"), //
				new WideRowMultiKey(15L, "15"), //
				false, 3);

		assertThat(foundTweets).hasSize(3);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("value4");

	}

	@Test
	public void should_find_values_by_range_with_exclusive_range() throws Exception
	{
		insert5Values();

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange( //
				new WideRowMultiKey(12L, "12"), //
				new WideRowMultiKey(15L, "15"), //
				false, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value4");
	}

	@Test
	public void should_find_values_by_range_with_exclusive_start_inclusive_end_reverse()
			throws Exception
	{
		insert5Values();

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange( //
				new WideRowMultiKey(14L, "14"), false, //
				new WideRowMultiKey(12L, "12"), true, //
				true, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start() throws Exception
	{
		insert5Values();

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange(null,
				new WideRowMultiKey(12L, "12"), false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start_and_end() throws Exception
	{
		insert5Values();

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange(null, null, false, 10);

		assertThat(foundTweets).hasSize(5);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(3).getValue()).isEqualTo("value4");
		assertThat(foundTweets.get(4).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<WideRowMultiKey, String>> iter = map.iterator(null, null, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("value1");
		assertThat(iter.next().getValue()).isEqualTo("value2");
		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.next().getValue()).isEqualTo("value4");
		assertThat(iter.next().getValue()).isEqualTo("value5");

	}

	@Test
	public void should_get_iterator_exclusive_bounds() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<WideRowMultiKey, String>> iter = map.iterator( //
				new WideRowMultiKey(12L, "12"), //
				new WideRowMultiKey(14L, "14"), //
				false, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_get_iterator_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<WideRowMultiKey, String>> iter = map.iterator(//
				new WideRowMultiKey(12L, "12"), //
				true, //
				new WideRowMultiKey(14L, "14"), //
				false, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("value2");
		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Values();

		map.remove(new WideRowMultiKey(11L, "11"));

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange(null, null, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value3");
	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Values();

		map.removeRange(new WideRowMultiKey(12L, "12"), new WideRowMultiKey(14L, "14"));

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange(null, null, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_remove_values_range_exclusive_bounds() throws Exception
	{
		insert5Values();

		map.removeRange(new WideRowMultiKey(12L, "12"), new WideRowMultiKey(15L, "15"), false);

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange(null, null, false, 10);

		assertThat(foundTweets).hasSize(3);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_remove_values_range_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		map.removeRange(new WideRowMultiKey(12L, "12"), true, new WideRowMultiKey(15L, "15"), false);

		List<KeyValue<WideRowMultiKey, String>> foundTweets = map.findRange(null, null, false, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value5");
	}

	private void insert3Values()
	{
		map.insert(new WideRowMultiKey(11L, "11"), "value1");
		map.insert(new WideRowMultiKey(12L, "12"), "value2");
		map.insert(new WideRowMultiKey(13L, "13"), "value3");
	}

	private void insert5Values()
	{
		map.insert(new WideRowMultiKey(11L, "11"), "value1");
		map.insert(new WideRowMultiKey(12L, "12"), "value2");
		map.insert(new WideRowMultiKey(13L, "13"), "value3");
		map.insert(new WideRowMultiKey(14L, "14"), "value4");
		map.insert(new WideRowMultiKey(15L, "15"), "value5");
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
