package integration.tests;

import static info.archinnov.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.common.CassandraDaoTest.getCluster;
import static info.archinnov.achilles.common.CassandraDaoTest.getKeyspace;
import static info.archinnov.achilles.common.CassandraDaoTest.getWideRowDao;
import static info.archinnov.achilles.serializer.SerializerUtils.LONG_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.holder.KeyValue;
import integration.tests.entity.ColumnFamilyBean;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * WideRowIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ColumnFamilyIT
{

	private final String ENTITY_PACKAGE = "integration.tests.entity";
	private GenericCompositeDao<Long, String> dao = getWideRowDao(LONG_SRZ, STRING_SRZ,
			normalizerAndValidateColumnFamilyName(ColumnFamilyBean.class.getName()));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private ColumnFamilyBean bean;

	private WideMap<Integer, String> map;

	private Long id = 452L;

	@Before
	public void setUp()
	{
		bean = new ColumnFamilyBean();
		bean.setId(id);
		bean = em.merge(bean);
		map = bean.getMap();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		bean = em.find(ColumnFamilyBean.class, id);
		map = bean.getMap();

		insert3Values();

		Composite startComp = new Composite();
		startComp.addComponent(0, 11, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 13, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), startComp,
				endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("value1");
		assertThat(columns.get(1).right).isEqualTo("value2");
		assertThat(columns.get(2).right).isEqualTo("value3");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		map.insert(1, "value1", 15);
		Composite startComp = new Composite();
		startComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 2, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<Composite, String>> columns = dao.findRawColumnsRange(bean.getId(), startComp,
				endComp, false, 10);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(15);
	}

	@Test
	public void should_not_find_value_after_ttl_expiration() throws Exception
	{
		map.insert(1, "value1", 1);

		Thread.sleep(1001);

		Composite startComp = new Composite();
		startComp.addComponent(0, 11, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 12, ComponentEquality.GREATER_THAN_EQUAL);
		List<HColumn<Composite, String>> columns = dao.findRawColumnsRange(bean.getId(), startComp,
				endComp, false, 10);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		insert3Values();

		assertThat(map.get(11)).isEqualTo("value1");
	}

	@Test
	public void should_find_values_by_range() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundMaps = map.find(11, 15, 10);

		assertThat(foundMaps).hasSize(5);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value1");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value2");
		assertThat(foundMaps.get(2).getValue()).isEqualTo("value3");
		assertThat(foundMaps.get(3).getValue()).isEqualTo("value4");
		assertThat(foundMaps.get(4).getValue()).isEqualTo("value5");

	}

	@Test
	public void should_find_values_by_range_with_limit() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundMaps = map.find(12, 15, 3);

		assertThat(foundMaps).hasSize(3);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value2");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value3");
		assertThat(foundMaps.get(2).getValue()).isEqualTo("value4");

	}

	@Test
	public void should_find_values_by_range_with_exclusive_range() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundMaps = map.findBoundsExclusive(12, 15, 10);

		assertThat(foundMaps).hasSize(2);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value3");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value4");
	}

	@Test
	public void should_find_values_by_range_with_exclusive_start_inclusive_end_reverse()
			throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundMaps = map.find( //
				14, false, //
				12, true, //
				true, 10);

		assertThat(foundMaps).hasSize(2);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value3");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundMaps = map.find(null, 12, 10);

		assertThat(foundMaps).hasSize(2);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value1");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start_and_end() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundMaps = map.find(null, null, 10);

		assertThat(foundMaps).hasSize(5);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value1");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value2");
		assertThat(foundMaps.get(2).getValue()).isEqualTo("value3");
		assertThat(foundMaps.get(3).getValue()).isEqualTo("value4");
		assertThat(foundMaps.get(4).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<Integer, String>> iter = map.iterator(null, null, 10);

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

		Iterator<KeyValue<Integer, String>> iter = map.iteratorBoundsExclusive(12, 14, 10);

		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_get_iterator_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<Integer, String>> iter = map.iterator(12, true, 14, false, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("value2");
		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Values();

		map.remove(11);

		List<KeyValue<Integer, String>> foundMaps = map.find(null, null, 10);

		assertThat(foundMaps).hasSize(2);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value2");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value3");
	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Values();

		map.remove(12, 14);

		List<KeyValue<Integer, String>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_remove_values_range_exclusive_bounds() throws Exception
	{
		insert5Values();

		map.removeBoundsExclusive(12, 15);

		List<KeyValue<Integer, String>> foundMaps = map.find(null, null, 10);

		assertThat(foundMaps).hasSize(3);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value1");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value2");
		assertThat(foundMaps.get(2).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_remove_values_range_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		map.remove(12, true, 15, false);

		List<KeyValue<Integer, String>> foundMaps = map.find(null, null, 10);

		assertThat(foundMaps).hasSize(2);
		assertThat(foundMaps.get(0).getValue()).isEqualTo("value1");
		assertThat(foundMaps.get(1).getValue()).isEqualTo("value5");
	}

	private void insert3Values()
	{
		map.insert(11, "value1");
		map.insert(12, "value2");
		map.insert(13, "value3");
	}

	private void insert5Values()
	{
		map.insert(11, "value1");
		map.insert(12, "value2");
		map.insert(13, "value3");
		map.insert(14, "value4");
		map.insert(15, "value5");
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
