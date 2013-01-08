package integration.tests;

import static fr.doan.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.common.CassandraDaoTest.getWideRowDao;
import static fr.doan.achilles.serializer.SerializerUtils.LONG_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import integration.tests.entity.CompleteBean;
import integration.tests.entity.CompleteBeanTestBuilder;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.holder.KeyValue;

/**
 * ExternalWideMapIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ExternalWideMapIT
{

	private final String ENTITY_PACKAGE = "integration.tests.entity";

	private GenericCompositeDao<Long, String> externalWideMapDao = getWideRowDao(LONG_SRZ,
			STRING_SRZ, normalizerAndValidateColumnFamilyName("ExternalWideMap"));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private CompleteBean bean;

	private WideMap<Integer, String> externalWideMap;

	@Before
	public void setUp()
	{
		bean = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").buid();
		bean = em.merge(bean);
		externalWideMap = bean.getExternalWideMap();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		insert3Values();

		Composite startComp = new Composite();
		startComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 3, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = externalWideMapDao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right).isEqualTo("value1");
		assertThat(columns.get(1).right).isEqualTo("value2");
		assertThat(columns.get(2).right).isEqualTo("value3");

	}

	@Test
	public void should_insert_value_with_ttl() throws Exception
	{
		externalWideMap.insert(1, "value1", 15);
		Composite startComp = new Composite();
		startComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 2, ComponentEquality.GREATER_THAN_EQUAL);

		List<HColumn<Composite, String>> columns = externalWideMapDao.findRawColumnsRange(
				bean.getId(), startComp, endComp, false, 10);

		assertThat(columns).hasSize(1);
		assertThat(columns.get(0).getTtl()).isEqualTo(15);
	}

	@Test
	public void should_not_find_value_after_ttl_expiration() throws Exception
	{
		externalWideMap.insert(1, "value1", 1);

		Thread.sleep(1001);

		Composite startComp = new Composite();
		startComp.addComponent(0, 1, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 2, ComponentEquality.GREATER_THAN_EQUAL);
		List<HColumn<Composite, String>> columns = externalWideMapDao.findRawColumnsRange(
				bean.getId(), startComp, endComp, false, 10);

		assertThat(columns).hasSize(0);
	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		insert3Values();

		assertThat(externalWideMap.get(1)).isEqualTo("value1");
	}

	@Test
	public void should_find_values_by_range() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(1, 5, false, 10);

		assertThat(foundValues).hasSize(5);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value1");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value2");
		assertThat(foundValues.get(2).getValue()).isEqualTo("value3");
		assertThat(foundValues.get(3).getValue()).isEqualTo("value4");
		assertThat(foundValues.get(4).getValue()).isEqualTo("value5");

	}

	@Test
	public void should_find_values_by_range_with_limit() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(2, 5, false, 3);

		assertThat(foundValues).hasSize(3);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value2");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value3");
		assertThat(foundValues.get(2).getValue()).isEqualTo("value4");

	}

	@Test
	public void should_find_values_by_range_with_exclusive_range() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(2, 5, false, false,
				10);

		assertThat(foundValues).hasSize(2);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value3");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value4");
	}

	@Test
	public void should_find_values_by_range_with_exclusive_start_inclusive_end_reverse()
			throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange( //
				4, false, //
				2, true, //
				true, 10);

		assertThat(foundValues).hasSize(2);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value3");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(null, 2, false, 10);

		assertThat(foundValues).hasSize(2);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value1");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value2");
	}

	@Test
	public void should_find_values_by_range_with_null_start_and_end() throws Exception
	{
		insert5Values();

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(null, null, false,
				10);

		assertThat(foundValues).hasSize(5);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value1");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value2");
		assertThat(foundValues.get(2).getValue()).isEqualTo("value3");
		assertThat(foundValues.get(3).getValue()).isEqualTo("value4");
		assertThat(foundValues.get(4).getValue()).isEqualTo("value5");
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<Integer, String>> iter = externalWideMap.iterator(null, null, false, 10);

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

		Iterator<KeyValue<Integer, String>> iter = externalWideMap.iterator(2, 4, false, false, 10);

		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_get_iterator_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<Integer, String>> iter = externalWideMap.iterator(2, true, 4, false,
				false, 10);

		assertThat(iter.next().getValue()).isEqualTo("value2");
		assertThat(iter.next().getValue()).isEqualTo("value3");
		assertThat(iter.hasNext()).isFalse();
	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Values();

		externalWideMap.remove(1);

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(null, null, false,
				10);

		assertThat(foundValues).hasSize(2);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value2");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value3");

		List<String> foundStrings = externalWideMapDao
				.findValuesRange(bean.getId(), null, false, 5);

		assertThat(foundStrings).hasSize(2);
		assertThat(foundStrings).containsExactly("value2", "value3");

	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Values();

		externalWideMap.removeRange(2, 4);

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(null, null, false,
				10);

		assertThat(foundValues).hasSize(2);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value1");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value5");

		List<String> foundStrings = externalWideMapDao
				.findValuesRange(bean.getId(), null, false, 5);

		assertThat(foundStrings).hasSize(2);
		assertThat(foundStrings).containsExactly("value1", "value5");
	}

	@Test
	public void should_remove_values_range_exclusive_bounds() throws Exception
	{
		insert5Values();

		externalWideMap.removeRange(2, 5, false);

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(null, null, false,
				10);

		assertThat(foundValues).hasSize(3);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value1");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value2");
		assertThat(foundValues.get(2).getValue()).isEqualTo("value5");

		List<String> foundStrings = externalWideMapDao
				.findValuesRange(bean.getId(), null, false, 5);

		assertThat(foundStrings).hasSize(3);
		assertThat(foundStrings).containsExactly("value1", "value2", "value5");
	}

	@Test
	public void should_remove_values_range_inclusive_start_exclusive_end() throws Exception
	{
		insert5Values();

		externalWideMap.removeRange(2, true, 5, false);

		List<KeyValue<Integer, String>> foundValues = externalWideMap.findRange(null, null, false,
				10);

		assertThat(foundValues).hasSize(2);
		assertThat(foundValues.get(0).getValue()).isEqualTo("value1");
		assertThat(foundValues.get(1).getValue()).isEqualTo("value5");

		List<String> foundStrings = externalWideMapDao
				.findValuesRange(bean.getId(), null, false, 5);

		assertThat(foundStrings).hasSize(2);
		assertThat(foundStrings).containsExactly("value1", "value5");
	}

	private void insert3Values()
	{
		externalWideMap.insert(1, "value1");
		externalWideMap.insert(2, "value2");
		externalWideMap.insert(3, "value3");
	}

	private void insert5Values()
	{
		externalWideMap.insert(1, "value1");
		externalWideMap.insert(2, "value2");
		externalWideMap.insert(3, "value3");
		externalWideMap.insert(4, "value4");
		externalWideMap.insert(5, "value5");
	}

	@After
	public void tearDown()
	{
		externalWideMapDao.truncate();
	}
}
