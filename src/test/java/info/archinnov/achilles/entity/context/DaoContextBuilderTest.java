package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.GenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.Counter;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import testBuilders.PropertyMetaTestBuilder;

/**
 * DaoContextBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class DaoContextBuilderTest
{

	@InjectMocks
	private DaoContextBuilder builder;

	@Mock
	private Cluster cluster;

	@Mock
	private Keyspace keyspace;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy consistencyPolicy;

	private ConfigurationContext configContext = new ConfigurationContext();

	private Map<Class<?>, EntityMeta<?>> entityMetaMap = new HashMap<Class<?>, EntityMeta<?>>();

	@Before
	public void setUp()
	{
		configContext.setConsistencyPolicy(consistencyPolicy);
		entityMetaMap.clear();
	}

	@Test
	public void should_build_counter_dao() throws Exception
	{
		DaoContext context = builder
				.buildDao(cluster, keyspace, entityMetaMap, configContext, true);

		CounterDao counterDao = context.getCounterDao();
		assertThat(counterDao).isNotNull();
		assertThat(Whitebox.getInternalState(counterDao, "policy")).isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(counterDao, "cluster")).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(counterDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(counterDao, "keySerializer")).isSameAs(COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(counterDao, "columnNameSerializer")).isSameAs(
				COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(counterDao, "valueSerializer")).isSameAs(LONG_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_entity_dao() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.build();

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setWideRow(false);
		entityMeta.setColumnFamilyName("cf");
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(new HashMap<String, PropertyMeta<?, ?>>());

		entityMetaMap.put(CompleteBean.class, entityMeta);

		DaoContext context = builder.buildDao(cluster, keyspace, entityMetaMap, configContext,
				false);

		GenericEntityDao<Long> entityDao = (GenericEntityDao<Long>) context.findEntityDao("cf");

		assertThat(entityDao).isNotNull();
		assertThat(entityDao.getColumnFamily()).isEqualTo("cf");
		assertThat(Whitebox.getInternalState(entityDao, "policy")).isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(entityDao, "cluster")).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(entityDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(entityDao, "keySerializer")).isSameAs(LONG_SRZ);
		assertThat(Whitebox.getInternalState(entityDao, "columnNameSerializer")).isSameAs(
				COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(entityDao, "valueSerializer")).isSameAs(STRING_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_wide_row_dao() throws Exception
	{
		PropertyMeta<UUID, String> geoPositionsMeta = PropertyMetaTestBuilder //
				.completeBean(UUID.class, String.class) //
				.field("id") //
				.externalCf("externalCf")//
				.type(PropertyType.WIDE_MAP) //
				.build();

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.build();

		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("geoPositions", geoPositionsMeta);

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setWideRow(true);
		entityMeta.setColumnFamilyName("cf");
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(propertyMetas);

		entityMetaMap.put(CompleteBean.class, entityMeta);

		DaoContext context = builder.buildDao(cluster, keyspace, entityMetaMap, configContext,
				false);

		GenericWideRowDao<Long, String> columnFamilyDao = (GenericWideRowDao<Long, String>) context
				.findWideRowDao("externalCf");

		assertThat(columnFamilyDao).isNotNull();
		assertThat(columnFamilyDao.getColumnFamily()).isEqualTo("externalCf");
		assertThat(Whitebox.getInternalState(columnFamilyDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "cluster")).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keySerializer")).isSameAs(LONG_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "columnNameSerializer")).isSameAs(
				COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "valueSerializer")).isSameAs(
				STRING_SRZ);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_wide_row_dao_with_object_value_type() throws Exception
	{
		PropertyMeta<UUID, UserBean> geoPositionsMeta = PropertyMetaTestBuilder //
				.completeBean(UUID.class, UserBean.class) //
				.field("friendsWideMap") //
				.externalCf("externalCf")//
				.type(PropertyType.WIDE_MAP) //
				.build();

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.build();

		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("friendsWideMap", geoPositionsMeta);

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setWideRow(true);
		entityMeta.setColumnFamilyName("cf");
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(propertyMetas);

		entityMetaMap.put(CompleteBean.class, entityMeta);

		DaoContext context = builder.buildDao(cluster, keyspace, entityMetaMap, configContext,
				false);

		GenericWideRowDao<Long, String> columnFamilyDao = (GenericWideRowDao<Long, String>) context
				.findWideRowDao("externalCf");

		assertThat(columnFamilyDao).isNotNull();
		assertThat(columnFamilyDao.getColumnFamily()).isEqualTo("externalCf");
		assertThat(Whitebox.getInternalState(columnFamilyDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "cluster")).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keySerializer")).isSameAs(LONG_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "columnNameSerializer")).isSameAs(
				COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "valueSerializer")).isSameAs(
				STRING_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_wide_row_dao_with_counter_type() throws Exception
	{
		PropertyMeta<String, Counter> geoPositionsMeta = PropertyMetaTestBuilder //
				.completeBean(String.class, Counter.class) //
				.field("popularTopics") //
				.externalCf("externalCf")//
				.type(PropertyType.COUNTER_WIDE_MAP) //
				.build();

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.build();

		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("popularTopics", geoPositionsMeta);

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setWideRow(true);
		entityMeta.setColumnFamilyName("cf");
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(propertyMetas);

		entityMetaMap.put(CompleteBean.class, entityMeta);

		DaoContext context = builder.buildDao(cluster, keyspace, entityMetaMap, configContext,
				false);

		GenericWideRowDao<Long, String> columnFamilyDao = (GenericWideRowDao<Long, String>) context
				.findWideRowDao("externalCf");

		assertThat(columnFamilyDao).isNotNull();
		assertThat(columnFamilyDao.getColumnFamily()).isEqualTo("externalCf");
		assertThat(Whitebox.getInternalState(columnFamilyDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "cluster")).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keySerializer")).isSameAs(LONG_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "columnNameSerializer")).isSameAs(
				COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "valueSerializer"))
				.isSameAs(LONG_SRZ);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_wide_row_dao_for_join_entity() throws Exception
	{
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdSerializer(LONG_SRZ);

		PropertyMeta<Long, UserBean> joinUsersMeta = PropertyMetaTestBuilder //
				.completeBean(Long.class, UserBean.class) //
				.field("joinUsers") //
				.externalCf("externalCf")//
				.joinMeta(joinMeta) //
				.type(PropertyType.JOIN_WIDE_MAP) //
				.idSerializer(LONG_SRZ) //
				.build();

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.build();

		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		propertyMetas.put("joinUsers", joinUsersMeta);

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setWideRow(true);
		entityMeta.setColumnFamilyName("cf");
		entityMeta.setIdMeta(idMeta);
		entityMeta.setPropertyMetas(propertyMetas);

		entityMetaMap.put(CompleteBean.class, entityMeta);

		DaoContext context = builder.buildDao(cluster, keyspace, entityMetaMap, configContext,
				false);

		GenericWideRowDao<Long, String> columnFamilyDao = (GenericWideRowDao<Long, String>) context
				.findWideRowDao("externalCf");

		assertThat(columnFamilyDao).isNotNull();
		assertThat(columnFamilyDao.getColumnFamily()).isEqualTo("externalCf");
		assertThat(Whitebox.getInternalState(columnFamilyDao, "policy"))
				.isSameAs(consistencyPolicy);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "cluster")).isSameAs(cluster);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keyspace")).isSameAs(keyspace);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "keySerializer")).isSameAs(LONG_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "columnNameSerializer")).isSameAs(
				COMPOSITE_SRZ);
		assertThat(Whitebox.getInternalState(columnFamilyDao, "valueSerializer"))
				.isSameAs(LONG_SRZ);
	}
}
