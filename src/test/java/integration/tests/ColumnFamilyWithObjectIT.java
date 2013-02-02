package integration.tests;

import static fr.doan.achilles.columnFamily.ColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.serializer.SerializerUtils.LONG_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.OBJECT_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import integration.tests.entity.ColumnFamilyBeanWithObject;
import integration.tests.entity.ColumnFamilyBeanWithObject.Holder;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.common.CassandraDaoTest;
import fr.doan.achilles.dao.GenericCompositeDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.holder.KeyValue;

/**
 * WideRowWithObjectIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ColumnFamilyWithObjectIT
{

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private GenericCompositeDao<Long, Holder> dao = CassandraDaoTest.getWideRowDao(LONG_SRZ,
			(Serializer) OBJECT_SRZ,
			normalizerAndValidateColumnFamilyName(ColumnFamilyBeanWithObject.class.getName()));

	private final String ENTITY_PACKAGE = "integration.tests.entity";
	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private ColumnFamilyBeanWithObject bean;

	private WideMap<Long, Holder> map;

	private Long id = 452L;

	@Before
	public void setUp()
	{
		bean = new ColumnFamilyBeanWithObject();
		bean.setId(id);
		bean = em.merge(bean);
		map = bean.getMap();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		insert3Values();

		Composite startComp = new Composite();
		startComp.addComponent(0, 11L, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 13L, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, Holder>> columns = dao.findColumnsRange(bean.getId(), startComp,
				endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(columns.get(0).right.getName()).isEqualTo("value1");
		assertThat(columns.get(1).right.getName()).isEqualTo("value2");
		assertThat(columns.get(2).right.getName()).isEqualTo("value3");

	}

	@Test
	public void should_get_value_by_key() throws Exception
	{
		insert3Values();

		assertThat(map.get(11L).getName()).isEqualTo("value1");
	}

	@Test
	public void should_find_values_by_range() throws Exception
	{
		insert5Values();

		List<KeyValue<Long, Holder>> foundTweets = map.find(11L, 15L, 10);

		assertThat(foundTweets).hasSize(5);
		assertThat(foundTweets.get(0).getValue().getName()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue().getName()).isEqualTo("value2");
		assertThat(foundTweets.get(2).getValue().getName()).isEqualTo("value3");
		assertThat(foundTweets.get(3).getValue().getName()).isEqualTo("value4");
		assertThat(foundTweets.get(4).getValue().getName()).isEqualTo("value5");

	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Values();

		Iterator<KeyValue<Long, Holder>> iter = map.iterator(null, null, 10);

		assertThat(iter.next().getValue().getName()).isEqualTo("value1");
		assertThat(iter.next().getValue().getName()).isEqualTo("value2");
		assertThat(iter.next().getValue().getName()).isEqualTo("value3");
		assertThat(iter.next().getValue().getName()).isEqualTo("value4");
		assertThat(iter.next().getValue().getName()).isEqualTo("value5");

	}

	@Test
	public void should_remove_value() throws Exception
	{
		insert3Values();

		map.remove(11L);

		List<KeyValue<Long, Holder>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue().getName()).isEqualTo("value2");
		assertThat(foundTweets.get(1).getValue().getName()).isEqualTo("value3");
	}

	@Test
	public void should_remove_values_range() throws Exception
	{
		insert5Values();

		map.remove(12L, 14L);

		List<KeyValue<Long, Holder>> foundTweets = map.find(null, null, 10);

		assertThat(foundTweets).hasSize(2);
		assertThat(foundTweets.get(0).getValue().getName()).isEqualTo("value1");
		assertThat(foundTweets.get(1).getValue().getName()).isEqualTo("value5");
	}

	private void insert3Values()
	{
		map.insert(11L, new Holder("value1"));
		map.insert(12L, new Holder("value2"));
		map.insert(13L, new Holder("value3"));
	}

	private void insert5Values()
	{
		map.insert(11L, new Holder("value1"));
		map.insert(12L, new Holder("value2"));
		map.insert(13L, new Holder("value3"));
		map.insert(14L, new Holder("value4"));
		map.insert(15L, new Holder("value5"));
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
