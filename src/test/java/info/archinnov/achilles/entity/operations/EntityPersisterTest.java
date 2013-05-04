package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.ALL;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.impl.ThriftPersisterImpl;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.CascadeType;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/**
 * EntityPersisterTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityPersisterTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private ThriftPersisterImpl persisterImpl;

	@Mock
	private EntityIntrospector introspector;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private CounterDao counterDao;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private CompleteBean bean = CompleteBeanTestBuilder.builder().randomId().buid();

	private PersistenceContext<Long> context;

	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();

	@Before
	public void setUp()
	{
		entityDaosMap.clear();
		context = PersistenceContextTestBuilder
				.context(entityMeta, counterDao, policy, CompleteBean.class, bean.getId())
				.entity(bean)//
				.entityDaosMap(entityDaosMap) //
				.build();
	}

	@Test
	public void should_persist_versionSerialUID() throws Exception
	{
		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(new HashMap<String, PropertyMeta<?, ?>>());

		persister.persist(context);

		verify(persisterImpl).batchPersistVersionSerialUID(context);
	}

	@Test
	public void should_persist_simple_property() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, String> simpleMeta = PropertyMetaTestBuilder.valueClass(String.class)
				.type(PropertyType.SIMPLE).build();
		propertyMetas.put("simple", simpleMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);

		persister.persist(context);

		verify(persisterImpl).batchPersistSimpleProperty(context, simpleMeta);
	}

	@Test
	public void should_cascade_persist() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.type(PropertyType.SIMPLE) //
				.build();
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta<Void, UserBean> propertyMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, UserBean.class) //
				.field("user") //
				.accesors() //
				.type(PropertyType.JOIN_SIMPLE)//
				.joinMeta(joinMeta)//
				.cascadeType(CascadeType.PERSIST) //
				.consistencyLevels(new Pair<ConsistencyLevel, ConsistencyLevel>(ALL, ALL)) //
				.build();

		propertyMetas.put("user", propertyMeta);

		Long joinId = RandomUtils.nextLong();

		UserBean user = new UserBean();

		when(introspector.getKey(bean, joinIdMeta)).thenReturn(joinId);
		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, propertyMeta.getGetter())).thenReturn(user);

		persister.cascadePersistOrEnsureExists(context, bean, propertyMeta.getJoinProperties());
		verify(persisterImpl).batchPersistJoinEntity(context, propertyMeta, user, persister);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_ensure_join_entity_exist() throws Exception
	{
		PropertyMeta<Void, Long> joinIdMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.type(PropertyType.SIMPLE) //
				.build();
		EntityMeta<Long> joinMeta = new EntityMeta<Long>();
		joinMeta.setIdMeta(joinIdMeta);
		joinMeta.setColumnFamilyName("cfName");
		GenericEntityDao<Long> entityDao = mock(GenericEntityDao.class);
		entityDaosMap.put("cfName", entityDao);
		Long joinId = RandomUtils.nextLong();
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);

		when(introspector.getKey(bean, joinIdMeta)).thenReturn(joinId);
		when(loader.loadVersionSerialUID(bean.getId(), entityDao)).thenReturn(joinId);
		context.getConfigContext().setEnsureJoinConsistency(true);

		persister.cascadePersistOrEnsureExists(context, bean, joinProperties);

	}

	@Test
	public void should_persist_list() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		ArrayList<String> list = new ArrayList<String>();

		PropertyMeta<Void, String> listMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)//
				.field("friends") //
				.accesors() //
				.type(PropertyType.LIST).build();
		propertyMetas.put("list", listMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, listMeta.getGetter())).thenReturn(list);
		persister.persist(context);

		verify(persisterImpl).batchPersistList(list, context, listMeta);
	}

	@Test
	public void should_persist_set() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		Set<String> set = new HashSet<String>();

		PropertyMeta<Void, String> setMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)//
				.field("followers") //
				.accesors() //
				.type(PropertyType.SET).build();
		propertyMetas.put("set", setMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, setMeta.getGetter())).thenReturn(set);
		persister.persist(context);

		verify(persisterImpl).batchPersistSet(set, context, setMeta);
	}

	@Test
	public void should_persist_map() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		Map<Integer, String> map = new HashMap<Integer, String>();

		PropertyMeta<Integer, String> mapMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, String.class)//
				.field("preferences") //
				.accesors() //
				.type(PropertyType.MAP).build();
		propertyMetas.put("map", mapMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, mapMeta.getGetter())).thenReturn(map);
		persister.persist(context);

		verify(persisterImpl).batchPersistMap(map, context, mapMeta);
	}

	@Test
	public void should_persist_join() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		UserBean user = new UserBean();

		PropertyMeta<Void, UserBean> joinMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, UserBean.class)//
				.field("user") //
				.accesors() //
				.type(PropertyType.JOIN_SIMPLE).build();
		propertyMetas.put("join", joinMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, joinMeta.getGetter())).thenReturn(user);
		persister.persist(context);

		verify(persisterImpl).batchPersistJoinEntity(context, joinMeta, user, persister);
	}

	@Test
	public void should_persist_join_collection() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		Set<String> joinSet = new HashSet<String>();

		PropertyMeta<Void, String> joinSetMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, String.class)//
				.field("followers") //
				.accesors() //
				.type(PropertyType.JOIN_SET).build();
		propertyMetas.put("joinSet", joinSetMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, joinSetMeta.getGetter())).thenReturn(joinSet);
		persister.persist(context);

		verify(persisterImpl).batchPersistJoinCollection(context, joinSetMeta, joinSet, persister);
	}

	@Test
	public void should_persist_join_map() throws Exception
	{
		HashMap<String, PropertyMeta<?, ?>> propertyMetas = new HashMap<String, PropertyMeta<?, ?>>();
		Map<Integer, String> joinMap = new HashMap<Integer, String>();

		PropertyMeta<Integer, String> joinMapMeta = PropertyMetaTestBuilder //
				.completeBean(Integer.class, String.class)//
				.field("preferences") //
				.accesors() //
				.type(PropertyType.JOIN_MAP).build();
		propertyMetas.put("joinMap", joinMapMeta);

		when(entityMeta.isWideRow()).thenReturn(false);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(introspector.getValueFromField(bean, joinMapMeta.getGetter())).thenReturn(joinMap);
		persister.persist(context);

		verify(persisterImpl).batchPersistJoinMap(context, joinMapMeta, joinMap, persister);
	}

	@Test
	public void should_remove() throws Exception
	{
		persister.remove(context);
		verify(persisterImpl).remove(context);
	}

	@Test
	public void should_remove_property_as_batch() throws Exception
	{
		PropertyMeta<Void, String> nameMeta = new PropertyMeta<Void, String>();

		persister.removePropertyBatch(context, nameMeta);
		verify(persisterImpl).removePropertyBatch(context, nameMeta);
	}

}
