package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.common.ThriftCassandraDaoTest.getEntityDao;
import static info.archinnov.achilles.table.TableNameNormalizer.normalizerAndValidateColumnFamilyName;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.common.ThriftCassandraDaoTest;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftEntityManager;
import info.archinnov.achilles.test.integration.entity.EntityWithObjectAsWideMapValue;
import info.archinnov.achilles.test.integration.entity.EntityWithObjectAsWideMapValue.Holder;
import info.archinnov.achilles.type.KeyValue;
import info.archinnov.achilles.type.Pair;
import info.archinnov.achilles.type.WideMap;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ObjectAsWideMapValueIT
 * 
 * @author DuyHai DOAN
 * 
 */
public class ObjectAsWideMapValueIT
{

	private ThriftGenericEntityDao dao = getEntityDao(
			normalizerAndValidateColumnFamilyName("bean_with_widemap_object"), Long.class);

	private ThriftEntityManager em = ThriftCassandraDaoTest.getEm();

	private EntityWithObjectAsWideMapValue bean;

	private Long id = 498L;

	private WideMap<Integer, Holder> holders;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp()
	{
		bean = new EntityWithObjectAsWideMapValue();
		bean.setId(id);
		bean.setName("name");

		bean = em.merge(bean);
		holders = bean.getHolders();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		insert3Holders();

		Composite startComp = new Composite();
		startComp.addComponent(0, 11, ComponentEquality.EQUAL);

		Composite endComp = new Composite();
		endComp.addComponent(0, 13, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = dao.findColumnsRange(bean.getId(), startComp,
				endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat((readHolder(columns.get(0).right)).getName()).isEqualTo("value1");
		assertThat((readHolder(columns.get(1).right)).getName()).isEqualTo("value2");
		assertThat((readHolder(columns.get(2).right)).getName()).isEqualTo("value3");
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Holders();

		Iterator<KeyValue<Integer, Holder>> iter = holders.iterator(null, null, 10);

		assertThat(iter.next().getValue().getName()).isEqualTo("value1");
		assertThat(iter.next().getValue().getName()).isEqualTo("value2");
		assertThat(iter.next().getValue().getName()).isEqualTo("value3");
		assertThat(iter.next().getValue().getName()).isEqualTo("value4");
		assertThat(iter.next().getValue().getName()).isEqualTo("value5");

	}

	private void insert3Holders()
	{
		holders.insert(11, new Holder("value1"));
		holders.insert(12, new Holder("value2"));
		holders.insert(13, new Holder("value3"));
	}

	private void insert5Holders()
	{
		holders.insert(11, new Holder("value1"));
		holders.insert(12, new Holder("value2"));
		holders.insert(13, new Holder("value3"));
		holders.insert(14, new Holder("value4"));
		holders.insert(15, new Holder("value5"));
	}

	private Holder readHolder(String string) throws Exception
	{
		return this.objectMapper.readValue(string, Holder.class);
	}

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
