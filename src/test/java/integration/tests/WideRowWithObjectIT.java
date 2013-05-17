package integration.tests;

import static info.archinnov.achilles.columnFamily.ThriftColumnFamilyHelper.normalizerAndValidateColumnFamilyName;
import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.Pair;
import info.archinnov.achilles.entity.type.WideMap;
import integration.tests.entity.WideRowBeanWithObject;
import integration.tests.entity.WideRowBeanWithObject.Holder;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ColumnFamilyWithObjectIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideRowWithObjectIT
{

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	private ThriftGenericWideRowDao<Long, String> dao = ThriftCassandraDaoTest.getColumnFamilyDao(
			LONG_SRZ, (Serializer) STRING_SRZ,
			normalizerAndValidateColumnFamilyName(WideRowBeanWithObject.class.getName()));

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private ObjectMapper objectMapper = new ObjectMapper();

	private WideRowBeanWithObject bean;

	private WideMap<Long, Holder> map;

	private Long id = 452L;

	@Before
	public void setUp()
	{
		bean = em.find(WideRowBeanWithObject.class, id);
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

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), startComp,
				endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(readHolder(columns.get(0).right).getName()).isEqualTo("value1");
		assertThat(readHolder(columns.get(1).right).getName()).isEqualTo("value2");
		assertThat(readHolder(columns.get(2).right).getName()).isEqualTo("value3");

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

	public Holder readHolder(String value) throws Exception
	{
		return objectMapper.readValue(value, Holder.class);
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
