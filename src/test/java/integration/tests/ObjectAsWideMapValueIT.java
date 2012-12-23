package integration.tests;

import static fr.doan.achilles.common.CassandraDaoTest.getCluster;
import static fr.doan.achilles.common.CassandraDaoTest.getEntityDao;
import static fr.doan.achilles.common.CassandraDaoTest.getKeyspace;
import static fr.doan.achilles.entity.metadata.PropertyType.WIDE_MAP;
import static fr.doan.achilles.entity.metadata.builder.EntityMetaBuilder.normalizeColumnFamilyName;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;

import mapping.entity.BeanWithObjectAsWideMapValue;
import mapping.entity.BeanWithObjectAsWideMapValue.Holder;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.factory.ThriftEntityManagerFactoryImpl;
import fr.doan.achilles.entity.manager.ThriftEntityManager;
import fr.doan.achilles.entity.type.WideMap;
import fr.doan.achilles.holder.KeyValue;

public class ObjectAsWideMapValueIT
{

	private final String ENTITY_PACKAGE = "mapping.entity";
	private GenericEntityDao<Long> dao = getEntityDao(LONG_SRZ,
			normalizeColumnFamilyName(BeanWithObjectAsWideMapValue.class.getCanonicalName()));

	private ThriftEntityManagerFactoryImpl factory = new ThriftEntityManagerFactoryImpl(
			getCluster(), getKeyspace(), ENTITY_PACKAGE, true);

	private ThriftEntityManager em = (ThriftEntityManager) factory.createEntityManager();

	private BeanWithObjectAsWideMapValue bean;

	private Long id = 498L;

	private WideMap<Integer, Holder> holders;

	@Before
	public void setUp()
	{
		bean = new BeanWithObjectAsWideMapValue();
		bean.setId(id);
		bean.setName("name");

		bean = em.merge(bean);
		holders = bean.getHolders();
	}

	@Test
	public void should_insert_values() throws Exception
	{

		insert3Holders();

		DynamicComposite startComp = buildComposite();
		startComp.addComponent(2, 11, ComponentEquality.EQUAL);

		DynamicComposite endComp = buildComposite();
		endComp.addComponent(2, 13, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<DynamicComposite, Object>> columns = dao.findColumnsRange(bean.getId(),
				startComp, endComp, false, 20);

		assertThat(columns).hasSize(3);
		assertThat(((Holder) columns.get(0).right).getName()).isEqualTo("value1");
		assertThat(((Holder) columns.get(1).right).getName()).isEqualTo("value2");
		assertThat(((Holder) columns.get(2).right).getName()).isEqualTo("value3");
	}

	@Test
	public void should_get_iterator() throws Exception
	{
		insert5Holders();

		Iterator<KeyValue<Integer, Holder>> iter = holders.iterator(null, null, false, 10);

		assertThat(iter.next().getValue().getName()).isEqualTo("value1");
		assertThat(iter.next().getValue().getName()).isEqualTo("value2");
		assertThat(iter.next().getValue().getName()).isEqualTo("value3");
		assertThat(iter.next().getValue().getName()).isEqualTo("value4");
		assertThat(iter.next().getValue().getName()).isEqualTo("value5");

	}

	private DynamicComposite buildComposite()
	{
		DynamicComposite startComp = new DynamicComposite();
		startComp.addComponent(0, WIDE_MAP.flag(), ComponentEquality.EQUAL);
		startComp.addComponent(1, "holders", ComponentEquality.EQUAL);
		return startComp;
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

	@After
	public void tearDown()
	{
		dao.truncate();
	}
}
