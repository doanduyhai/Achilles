package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.CassandraDaoTest;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import integration.tests.entity.ColumnFamilyMultiKey;
import integration.tests.entity.MultiKeyColumnFamilyBean;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.Composite;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * MultiKeyWideRowIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyColumnFamilyIT
{

	private GenericColumnFamilyDao<Long, String> dao = CassandraDaoTest.getColumnFamilyDao(LONG_SRZ,
			STRING_SRZ,
			normalizerAndValidateColumnFamilyName(MultiKeyColumnFamilyBean.class.getName()));

	private ThriftEntityManager em = CassandraDaoTest.getEm();

	private MultiKeyColumnFamilyBean bean;

	private WideMap<ColumnFamilyMultiKey, String> map;

	private Long id = 452L;

	@Before
	public void setUp()
	{
		bean = new MultiKeyColumnFamilyBean();
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

		assertThat(map.get(new ColumnFamilyMultiKey(11L, "11"))).isEqualTo("value1");
	}

	@Test
	public void should_find_values_by_range() throws Exception
	{
		insert5Values();

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find( //
				new ColumnFamilyMultiKey(11L, "11"), //
				new ColumnFamilyMultiKey(15L, "15"), //
				10);

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

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find( //
				new ColumnFamilyMultiKey(12L, "12"), //
				new ColumnFamilyMultiKey(15L, "15"), //
				3);

		assertThat(foundTweets).hasSize(3);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("value4");

	}

	@Test
	public void should_find_values_by_range_with_exclusive_range() throws Exception
	{
		insert5Values();

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.findBoundsExclusive( //
				new ColumnFamilyMultiKey(12L, "12"), //
				new ColumnFamilyMultiKey(15L, "15"), //
				10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value4");
	}

	@Test
	public void should_find_values_by_range_with_exclusive_start_inclusive_end_reverse()
			throws Exception
	{
		insert5Values();

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(new ColumnFamilyMultiKey(14L, "14"), new ColumnFamilyMultiKey(12L, "12"), 10, 
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value3");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start() throws Exception
	{
		insert5Values();

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(null,
				new ColumnFamilyMultiKey(12L, "12"), 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start_and_end() throws Exception
	{
		insert5Values();

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(null, null, 10);

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

		Iterator<KeyValue<ColumnFamilyMultiKey, String>> iter = map.iterator(null, null, 10);

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

		Iterator<KeyValue<ColumnFamilyMultiKey, String>> iter = map.iteratorBoundsExclusive( //
				new ColumnFamilyMultiKey(12L, "12"), //
				new ColumnFamilyMultiKey(14L, "14"), //
				10);

		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_get_iterator_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<ColumnFamilyMultiKey, String>> iter = map.iterator(new ColumnFamilyMultiKey(12L, "12"), 
				new ColumnFamilyMultiKey(14L, "14"), 10,
				BoundingMode.INCLUSIVE_START_BOUND_ONLY, OrderingMode.ASCENDING);

		assertThat(iter.next().getValue()).isEqualTo("value2");
		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Values();

		map.remove(new ColumnFamilyMultiKey(11L, "11"));

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value3");
	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Values();

		map.remove(new ColumnFamilyMultiKey(12L, "12"), new ColumnFamilyMultiKey(14L, "14"));

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_remove_values_range_exclusive_bounds() throws Exception
	{
		insert5Values();

		map.removeBoundsExclusive(new ColumnFamilyMultiKey(12L, "12"), new ColumnFamilyMultiKey(
				15L, "15"));

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(3);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value2");
		assertThat(foundTweets.get(2).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_remove_values_range_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		map.remove(new ColumnFamilyMultiKey(12L, "12"), new ColumnFamilyMultiKey(15L, "15"), BoundingMode.INCLUSIVE_START_BOUND_ONLY);

		List<KeyValue<ColumnFamilyMultiKey, String>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value5");
	}

	private void insert3Values()
	{
		map.insert(new ColumnFamilyMultiKey(11L, "11"), "value1");
		map.insert(new ColumnFamilyMultiKey(12L, "12"), "value2");
		map.insert(new ColumnFamilyMultiKey(13L, "13"), "value3");
	}

	private void insert5Values()
	{
		map.insert(new ColumnFamilyMultiKey(11L, "11"), "value1");
		map.insert(new ColumnFamilyMultiKey(12L, "12"), "value2");
		map.insert(new ColumnFamilyMultiKey(13L, "13"), "value3");
		map.insert(new ColumnFamilyMultiKey(14L, "14"), "value4");
		map.insert(new ColumnFamilyMultiKey(15L, "15"), "value5");
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
