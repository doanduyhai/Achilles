package info.archinnov.achilles.wrapper;

import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.context.PersistenceContextTestBuilder;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.ExternalWideMapProperties;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.KeyValueIterator;
import info.archinnov.achilles.entity.type.WideMap.BoundingMode;
import info.archinnov.achilles.entity.type.WideMap.OrderingMode;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.AchillesJoinSliceIterator;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.serializer.SerializerUtils;

import java.lang.reflect.Method;
import java.util.List;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import testBuilders.PropertyMetaTestBuilder;

/*
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class JoinExternalWideMapWrapperTest
{
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private JoinExternalWideMapWrapper<Long, Long, Integer, CompleteBean> wrapper;

	@Mock
	private GenericColumnFamilyDao<Long, Long> dao;

	@Mock
	private GenericEntityDao<Long> joinDao;

	@Mock
	private PropertyMeta<Integer, CompleteBean> propertyMeta;

	@Mock
	private CompositeKeyFactory compositeKeyFactory;

	@Mock
	private EntityPersister persister;

	@Mock
	private EntityLoader loader;

	@Mock
	private EntityProxifier proxifier;

	@Mock
	private CompositeHelper compositeHelper;

	@Mock
	private KeyValueFactory keyValueFactory;

	@Mock
	private IteratorFactory iteratorFactory;

	@Mock
	private AchillesInterceptor<Long> interceptor;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Long> joinMutator;

	@Mock
	private CounterDao counterDao;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	@Mock
	private GenericEntityDao<Long> entityDao;

	private EntityMeta<Long> entityMeta;

	private PersistenceContext<Long> context;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().buid();

	private Long id = 7425L;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception
	{
		wrapper.setId(id);

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.completeBean(Void.class, Long.class) //
				.field("id") //
				.type(PropertyType.SIMPLE) //
				.accesors() //
				.build();

		entityMeta = new EntityMeta<Long>();
		entityMeta.setIdMeta(idMeta);
		context = PersistenceContextTestBuilder //
				.context(entityMeta, counterDao, policy, CompleteBean.class, entity.getId()) //
				.entity(entity) //
				.build();
		wrapper.setContext(context);
		ExternalWideMapProperties<Long> properties = new ExternalWideMapProperties<Long>(
				"external_cf", SerializerUtils.LONG_SRZ);
		when((ExternalWideMapProperties<Long>) propertyMeta.getExternalWideMapProperties())
				.thenReturn(properties);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_value() throws Exception
	{
		int key = 4567;
		Composite comp = new Composite();

		when(compositeKeyFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
		when((EntityMeta<Long>) propertyMeta.joinMeta()).thenReturn(entityMeta);
		when((Class<CompleteBean>) propertyMeta.getValueClass()).thenReturn(CompleteBean.class);

		when(dao.getValue(id, comp)).thenReturn(entity.getId());
		when(loader.load(any(PersistenceContext.class))).thenReturn(entity);
		when(proxifier.buildProxy(eq(entity), any(PersistenceContext.class))).thenReturn(entity);

		CompleteBean actual = wrapper.get(key);

		assertThat(actual).isSameAs(entity);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_insert_value_and_entity_when_insertable() throws Exception
	{

		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(PERSIST);

		int key = 4567;
		Composite comp = new Composite();

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when((EntityMeta<Long>) propertyMeta.joinMeta()).thenReturn(entityMeta);
		when(
				persister.cascadePersistOrEnsureExists(any(PersistenceContext.class), eq(entity),
						eq(joinProperties))).thenReturn(entity.getId());

		when(compositeKeyFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
		when(joinDao.buildMutator()).thenReturn(joinMutator);
		wrapper.insert(key, userBean);

		verify(dao).setValue(id, comp, userId);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_insert_value_in_batch_mode_and_entity_when_insertable() throws Exception
	{

		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(PERSIST);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		Composite comp = new Composite();

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(compositeKeyFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
		when(propertyMeta.getPropertyName()).thenReturn("joinProperty");
		when(interceptor.getMutatorForProperty("joinProperty")).thenReturn((Mutator) joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties, joinMutator))
				.thenReturn(userId);
		when(interceptor.isBatchMode()).thenReturn(true);
		when(interceptor.getMutator()).thenReturn((Mutator) mutator);
		wrapper.insert(key, userBean);

		verify(dao).setValueBatch(id, comp, userId, mutator);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_exception_when_trying_to_persist_null_entity() throws Exception
	{
		int key = 4567;
		wrapper.insert(key, null);
	}

	@Test
	public void should_insert_value_and_entity_with_ttl() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(ALL);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		Composite comp = new Composite();

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(compositeKeyFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
		when(joinDao.buildMutator()).thenReturn(joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties)).thenReturn(userId);
		when(interceptor.isBatchMode()).thenReturn(false);
		wrapper.insert(key, userBean, 150);

		verify(dao).setValue(id, comp, userId, 150);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_insert_value_in_batch_mode_and_entity_with_ttl() throws Exception
	{
		JoinProperties joinProperties = prepareJoinProperties();
		joinProperties.addCascadeType(ALL);

		int key = 4567;
		UserBean userBean = new UserBean();
		long userId = 475L;
		userBean.setUserId(userId);
		Composite comp = new Composite();

		when(propertyMeta.getJoinProperties()).thenReturn((JoinProperties) joinProperties);
		when(compositeKeyFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);
		when(propertyMeta.getPropertyName()).thenReturn("joinProperty");
		when(interceptor.getMutatorForProperty("joinProperty")).thenReturn((Mutator) joinMutator);
		when(persister.cascadePersistOrEnsureExists(userBean, joinProperties, joinMutator))
				.thenReturn(userId);
		when(interceptor.isBatchMode()).thenReturn(true);
		when(interceptor.getMutator()).thenReturn((Mutator) mutator);

		wrapper.insert(key, userBean, 150);

		verify(dao).setValueBatch(id, comp, userId, 150, mutator);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_keyvalue_range() throws Exception
	{
		int start = 7, end = 5, count = 10;

		Composite startComp = new Composite(), endComp = new Composite();

		when(
				compositeKeyFactory.createForQuery(propertyMeta, start, end,
						BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});
		List<HColumn<Composite, ?>> hColumns = mock(List.class);
		when(
				dao.findRawColumnsRange(id, startComp, endComp, count,
						OrderingMode.DESCENDING.isReverse())).thenReturn((List) hColumns);
		List<KeyValue<Integer, UserBean>> values = mock(List.class);
		when(keyValueFactory.createJoinKeyValueListForComposite(propertyMeta, hColumns))
				.thenReturn(values);

		List<KeyValue<Integer, UserBean>> expected = wrapper.find(start, end, count,
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		verify(compositeHelper).checkBounds(propertyMeta, start, end, OrderingMode.DESCENDING);
		assertThat(expected).isSameAs(values);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_values_range() throws Exception
	{
		int start = 7, end = 5, count = 10;

		Composite startComp = new Composite(), endComp = new Composite();

		when(
				compositeKeyFactory.createForQuery(propertyMeta, start, end,
						BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});
		List<HColumn<Composite, ?>> hColumns = mock(List.class);
		when(
				dao.findRawColumnsRange(id, startComp, endComp, count,
						OrderingMode.DESCENDING.isReverse())).thenReturn((List) hColumns);
		List<UserBean> values = mock(List.class);
		when(keyValueFactory.createJoinValueListForComposite(propertyMeta, hColumns)).thenReturn(
				values);

		List<UserBean> expected = wrapper.findValues(start, end, count,
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		verify(compositeHelper).checkBounds(propertyMeta, start, end, OrderingMode.DESCENDING);
		assertThat(expected).isSameAs(values);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_find_keys_range() throws Exception
	{
		int start = 7, end = 5, count = 10;

		Composite startComp = new Composite(), endComp = new Composite();

		when(
				compositeKeyFactory.createForQuery(propertyMeta, start, end,
						BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});
		List<HColumn<Composite, ?>> hColumns = mock(List.class);
		when(
				dao.findRawColumnsRange(id, startComp, endComp, count,
						OrderingMode.DESCENDING.isReverse())).thenReturn((List) hColumns);
		List<Integer> values = mock(List.class);
		when(keyValueFactory.createKeyListForComposite(propertyMeta, hColumns)).thenReturn(values);

		List<Integer> expected = wrapper.findKeys(start, end, count,
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		verify(compositeHelper).checkBounds(propertyMeta, start, end, OrderingMode.DESCENDING);
		assertThat(expected).isSameAs(values);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_get_iterator() throws Exception
	{
		int start = 7, end = 5, count = 10;
		Composite startComp = new Composite(), endComp = new Composite();

		when(
				compositeKeyFactory.createForQuery(propertyMeta, start, end,
						BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING))
				.thenReturn(new Composite[]
				{
						startComp,
						endComp
				});

		AchillesJoinSliceIterator<Long, Composite, Long, Integer, UserBean> iterator = mock(AchillesJoinSliceIterator.class);
		when(
				dao.getJoinColumnsIterator(propertyMeta, id, startComp, endComp,
						OrderingMode.DESCENDING.isReverse(), count)).thenReturn(iterator);

		KeyValueIterator<Integer, UserBean> keyValueIterator = mock(KeyValueIterator.class);
		when(iteratorFactory.createKeyValueJoinIteratorForComposite(iterator, propertyMeta))
				.thenReturn(keyValueIterator);

		KeyValueIterator<Integer, UserBean> expected = wrapper.iterator(start, end, count,
				BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.DESCENDING);

		assertThat(expected).isSameAs(keyValueIterator);
	}

	@Test
	public void should_remove() throws Exception
	{
		int key = 4567;
		Composite comp = new Composite();

		when(compositeKeyFactory.createBaseComposite(propertyMeta, key)).thenReturn(comp);

		wrapper.remove(key);

		verify(dao).removeColumn(id, comp);
	}

	@Test
	public void should_remove_range() throws Exception
	{

		int start = 7, end = 5;

		Composite startComp = new Composite(), endComp = new Composite();

		when(
				compositeKeyFactory.createForQuery(propertyMeta, start, end,
						BoundingMode.INCLUSIVE_END_BOUND_ONLY, OrderingMode.ASCENDING)).thenReturn(
				new Composite[]
				{
						startComp,
						endComp
				});

		wrapper.remove(start, end, BoundingMode.INCLUSIVE_END_BOUND_ONLY);

		verify(compositeHelper).checkBounds(propertyMeta, start, end, OrderingMode.ASCENDING);
		verify(dao).removeColumnRange(id, startComp, endComp);

	}

	@Test
	public void should_remove_first() throws Exception
	{
		wrapper.removeFirst(15);

		verify(dao).removeColumnRange(id, null, null, false, 15);
	}

	@Test
	public void should_remove_last() throws Exception
	{
		wrapper.removeLast(9);

		verify(dao).removeColumnRange(id, null, null, true, 9);
	}

	private JoinProperties prepareJoinProperties() throws Exception
	{
		EntityMeta<Long> joinEntityMeta = new EntityMeta<Long>();
		joinEntityMeta.setClassName("canonicalClassName");
		joinEntityMeta.setEntityDao(joinDao);

		Method idGetter = UserBean.class.getDeclaredMethod("getUserId");
		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(PropertyType.SIMPLE);
		idMeta.setGetter(idGetter);
		joinEntityMeta.setIdMeta(idMeta);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinEntityMeta);

		return joinProperties;
	}
}
