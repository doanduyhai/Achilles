package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.entity.metadata.PropertyType.JOIN_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.builder.EntityMetaTestBuilder;
import info.archinnov.achilles.holder.KeyValue;
import info.archinnov.achilles.proxy.builder.EntityProxyBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.ColumnFamilyBean;
import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import testBuilders.CompositeTestBuilder;
import testBuilders.PropertyMetaTestBuilder;

/**
 * EntityLoaderTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest
{

	@InjectMocks
	private EntityLoader loader;

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private PropertyMeta<Void, String> listMeta;

	@Mock
	private PropertyMeta<Void, String> setMeta;

	@Mock
	private PropertyMeta<Integer, String> mapMeta;

	@Mock
	private PropertyMeta<Void, CompleteBean> joinMeta;

	@Mock
	private EntityMapper mapper;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private EntityProxyBuilder interceptorBuilder;

	@Mock
	private EntityHelper helper;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Captor
	ArgumentCaptor<UserBean> userBeanCaptor;

	@Captor
	ArgumentCaptor<Long> idCaptor;

	private CompleteBean bean;

	@Before
	public void setUp()
	{
		bean = CompleteBeanTestBuilder.builder().buid();
		ReflectionTestUtils.setField(loader, "interceptorBuilder", interceptorBuilder);
		ReflectionTestUtils.setField(loader, "helper", helper);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_load_entity() throws Exception
	{
		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
		columns.add(new Pair<DynamicComposite, String>(new DynamicComposite(), ""));
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		doNothing().when(helper).setValueToField(any(CompleteBean.class), eq(idSetter),
				idCaptor.capture());

		loader.load(CompleteBean.class, 1L, entityMeta);

		verify(mapper).setEagerPropertiesToEntity(eq(1L), eq(columns), eq(entityMeta),
				any(CompleteBean.class));

		assertThat(idCaptor.getValue()).isEqualTo(1L);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_load_widerow() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		Method idSetter = ColumnFamilyBean.class.getDeclaredMethod("setId", Long.class);

		when(entityMeta.isWideRow()).thenReturn(true);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getSetter()).thenReturn(idSetter);
		doNothing().when(helper).setValueToField(any(CompleteBean.class), eq(idSetter),
				idCaptor.capture());

		loader.load(ColumnFamilyBean.class, 452L, entityMeta);

		assertThat(idCaptor.getValue()).isEqualTo(452L);
	}

	@Test
	public void should_not_load_entity_because_not_found() throws Exception
	{
		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();

		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		CompleteBean bean = loader.load(CompleteBean.class, 1L, entityMeta);

		assertThat(bean).isNull();
		verifyZeroInteractions(mapper);
	}

	@Test(expected = RuntimeException.class)
	public void should_exception_when_error() throws Exception
	{

		when(entityMeta.getEntityDao()).thenThrow(new RuntimeException());
		loader.load(CompleteBean.class, 1L, entityMeta);
	}

	@Test
	public void should_load_simple_property() throws Exception
	{
		when(propertyMeta.getPropertyName()).thenReturn("name");
		when(propertyMeta.getValueFromString("name")).thenReturn("name");
		when(propertyMeta.type()).thenReturn(SIMPLE);
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, 0, ComponentEquality.EQUAL);
		composite.addComponent(1, 0, ComponentEquality.EQUAL);

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(composite);
		when(dao.getValue(1L, composite)).thenReturn("name");

		String value = loader.loadSimpleProperty(1L, dao, propertyMeta);
		assertThat(value).isEqualTo("name");
	}

	@Test
	public void should_load_list_property() throws Exception
	{
		when(listMeta.getPropertyName()).thenReturn("friends");
		when(listMeta.type()).thenReturn(LAZY_LIST);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		List<Pair<DynamicComposite, String>> friends = new ArrayList<Pair<DynamicComposite, String>>();
		friends.add(new Pair<DynamicComposite, String>(start, "foo"));
		friends.add(new Pair<DynamicComposite, String>(end, "bar"));

		when(keyFactory.createBaseForQuery(listMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(listMeta, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(friends);

		when(listMeta.newListInstance()).thenReturn(new ArrayList<String>());
		when(listMeta.getValueFromString("foo")).thenReturn("foo");
		when(listMeta.getValueFromString("bar")).thenReturn("bar");

		List<String> value = loader.loadListProperty(1L, dao, listMeta);

		assertThat(value).hasSize(2);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_load_set_property() throws Exception
	{
		when(setMeta.getPropertyName()).thenReturn("followers");
		when(setMeta.type()).thenReturn(SET);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		List<Pair<DynamicComposite, String>> followers = new ArrayList<Pair<DynamicComposite, String>>();
		followers.add(new Pair<DynamicComposite, String>(start, "George"));
		followers.add(new Pair<DynamicComposite, String>(end, "Paul"));

		when(keyFactory.createBaseForQuery(setMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(setMeta, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(followers);

		when(setMeta.newSetInstance()).thenReturn(new HashSet<String>());
		when(setMeta.getValueFromString("George")).thenReturn("George");
		when(setMeta.getValueFromString("Paul")).thenReturn("Paul");

		Set<String> value = loader.loadSetProperty(1L, dao, setMeta);

		assertThat(value).hasSize(2);
		assertThat(value).contains("George", "Paul");
	}

	@Test
	public void should_load_map_property() throws Exception
	{
		when(mapMeta.getPropertyName()).thenReturn("preferences");
		when(mapMeta.type()).thenReturn(LAZY_MAP);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite middle = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		List<Pair<DynamicComposite, String>> preferences = new ArrayList<Pair<DynamicComposite, String>>();

		KeyValue<Integer, String> keyValue1 = new KeyValue<Integer, String>(1, "FR");
		KeyValue<Integer, String> keyValue2 = new KeyValue<Integer, String>(2, "Paris");
		KeyValue<Integer, String> keyValue3 = new KeyValue<Integer, String>(3, "75014");

		String stringKeyValue1 = writeToString(keyValue1);
		String stringKeyValue2 = writeToString(keyValue2);
		String stringKeyValue3 = writeToString(keyValue3);

		preferences.add(new Pair<DynamicComposite, String>(start, stringKeyValue1));
		preferences.add(new Pair<DynamicComposite, String>(middle, stringKeyValue2));
		preferences.add(new Pair<DynamicComposite, String>(end, stringKeyValue3));

		when(keyFactory.createBaseForQuery(mapMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(mapMeta, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE))
				.thenReturn(preferences);

		when(mapMeta.getKeyClass()).thenReturn(Integer.class);

		when(mapMeta.getKeyValueFromString(stringKeyValue1)).thenReturn(keyValue1);
		when(mapMeta.getKeyValueFromString(stringKeyValue2)).thenReturn(keyValue2);
		when(mapMeta.getKeyValueFromString(stringKeyValue3)).thenReturn(keyValue3);

		when(mapMeta.getValueFromString(keyValue1.getValue())).thenReturn("FR");
		when(mapMeta.getValueFromString(keyValue2.getValue())).thenReturn("Paris");
		when(mapMeta.getValueFromString(keyValue3.getValue())).thenReturn("75014");

		Map<Integer, String> value = loader.loadMapProperty(1L, dao, mapMeta);

		assertThat(value).hasSize(3);
		assertThat(value).containsKey(1);
		assertThat(value).containsValue("FR");
		assertThat(value).containsKey(2);
		assertThat(value).containsValue("Paris");
		assertThat(value).containsKey(3);
		assertThat(value).containsValue("75014");
	}

	@Test
	public void should_load_simple_property_into_object() throws Exception
	{
		when(propertyMeta.type()).thenReturn(SIMPLE);

		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, 0, ComponentEquality.EQUAL);
		composite.addComponent(1, 0, ComponentEquality.EQUAL);

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(composite);

		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, propertyMeta);

		verify(spy).loadSimpleProperty(1L, dao, propertyMeta);
		verify(propertyMeta).getSetter();
	}

	@Test
	public void should_load_simple_lazy_property_into_object() throws Exception
	{
		when(propertyMeta.type()).thenReturn(LAZY_SIMPLE);

		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, 0, ComponentEquality.EQUAL);
		composite.addComponent(1, 0, ComponentEquality.EQUAL);

		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(composite);

		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, propertyMeta);

		verify(spy).loadSimpleProperty(1L, dao, propertyMeta);
		verify(propertyMeta).getSetter();
	}

	@Test
	public void should_load_list_property_into_object() throws Exception
	{
		when(listMeta.type()).thenReturn(LIST);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, listMeta);

		verify(spy).loadListProperty(1L, dao, listMeta);
		verify(listMeta).getSetter();
	}

	@Test
	public void should_load_list_lazy_property_into_object() throws Exception
	{
		when(listMeta.type()).thenReturn(LAZY_LIST);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, listMeta);

		verify(spy).loadListProperty(1L, dao, listMeta);
		verify(listMeta).getSetter();
	}

	@Test
	public void should_load_set_property_into_object() throws Exception
	{
		when(setMeta.type()).thenReturn(SET);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, setMeta);

		verify(spy).loadSetProperty(1L, dao, setMeta);
		verify(setMeta).getSetter();
	}

	@Test
	public void should_load_set_lazy_property_into_object() throws Exception
	{
		when(setMeta.type()).thenReturn(LAZY_SET);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, setMeta);

		verify(spy).loadSetProperty(1L, dao, setMeta);
		verify(setMeta).getSetter();
	}

	@Test
	public void should_load_map_property_into_object() throws Exception
	{
		when(mapMeta.type()).thenReturn(MAP);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, mapMeta);

		verify(spy).loadMapProperty(1L, dao, mapMeta);
		verify(mapMeta).getSetter();
	}

	@Test
	public void should_load_map_lazy_property_into_object() throws Exception
	{
		when(mapMeta.type()).thenReturn(LAZY_MAP);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, mapMeta);

		verify(spy).loadMapProperty(1L, dao, mapMeta);
		verify(mapMeta).getSetter();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_load_join_entity_into_object() throws Exception
	{
		Long joinId = 45L;

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(dao);
		Method idSetter = UserBean.class.getDeclaredMethod("setUserId", Long.class);

		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(SIMPLE);
		idMeta.setSetter(idSetter);
		entityMeta.setIdMeta(idMeta);

		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(entityMeta);

		PropertyMeta<Integer, UserBean> joinPropertyMeta = new PropertyMeta<Integer, UserBean>();
		joinPropertyMeta.setType(PropertyType.JOIN_WIDE_MAP);
		joinPropertyMeta.setSingleKey(true);

		joinPropertyMeta.setValueClass(UserBean.class);
		joinPropertyMeta.setJoinProperties(joinProperties);

		UserBean userBean = new UserBean();

		List<Pair<DynamicComposite, String>> columns = mock(List.class);
		when(columns.size()).thenReturn(1);
		when(dao.eagerFetchEntity(joinId)).thenReturn(columns);
		doNothing().when(helper).setValueToField(any(UserBean.class), eq(idSetter),
				idCaptor.capture());
		when(interceptorBuilder.build(userBeanCaptor.capture(), eq(entityMeta))).thenReturn(
				userBean);

		UserBean expected = this.loader.loadJoinEntity(UserBean.class, joinId, entityMeta);

		assertThat(expected).isSameAs(userBean);
		assertThat(idCaptor.getValue()).isEqualTo(joinId);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_load_join_column() throws Throwable
	{
		Long joinId = 45L, id = 545L;

		PropertyMeta<Void, Long> idMeta = PropertyMetaTestBuilder //
				.of(UserBean.class, Void.class, Long.class) //
				.type(SIMPLE) //
				.field("userId") //
				.build();

		EntityMeta<Long> entityMeta = EntityMetaTestBuilder.entityMeta().build(
				mock(ExecutingKeyspace.class), dao, CompleteBean.class, null);
		entityMeta.setIdMeta(idMeta);

		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(entityMeta);

		PropertyMeta<Void, UserBean> joinPropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Void.class, UserBean.class) //
				.field("user") //
				.type(JOIN_SIMPLE) //
				.joinMeta(entityMeta) //
				.build();

		UserBean userBean = new UserBean();
		userBean.setUserId(joinId);

		List<Pair<DynamicComposite, String>> columns = mock(List.class);
		when(columns.size()).thenReturn(1);

		DynamicComposite comp = CompositeTestBuilder.builder().values(0, 0).buildDynamic();

		when(keyFactory.createBaseForQuery(joinPropertyMeta, EQUAL)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn(joinId.toString());
		when(dao.eagerFetchEntity(joinId)).thenReturn(columns);
		doNothing().when(helper).setValueToField(any(UserBean.class), eq(idMeta.getSetter()),
				idCaptor.capture());
		when(interceptorBuilder.build(userBeanCaptor.capture(), eq(entityMeta))).thenReturn(
				userBean);

		doNothing().when(helper).setValueToField(eq(bean), eq(joinPropertyMeta.getSetter()),
				userBeanCaptor.capture());

		loader.loadPropertyIntoObject(bean, id, dao, joinPropertyMeta);

		List<UserBean> userBeans = userBeanCaptor.getAllValues();

		verify(mapper).setEagerPropertiesToEntity(joinId, columns, entityMeta, userBeans.get(0));
		verify(helper).setValueToField(userBeans.get(0), idMeta.getSetter(), joinId);
		assertThat(userBeans.get(0)).isInstanceOf(UserBean.class);
		assertThat(idCaptor.getValue()).isEqualTo(joinId);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_null_when_null_join_column() throws Exception
	{
		Long id = 545L;

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(dao);
		Method idSetter = UserBean.class.getDeclaredMethod("setUserId", Long.class);
		Method userSetter = CompleteBean.class.getDeclaredMethod("setUser", UserBean.class);

		PropertyMeta<Void, Long> idMeta = new PropertyMeta<Void, Long>();
		idMeta.setType(SIMPLE);
		idMeta.setSetter(idSetter);
		entityMeta.setIdMeta(idMeta);

		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(entityMeta);

		PropertyMeta<Void, UserBean> joinPropertyMeta = new PropertyMeta<Void, UserBean>();
		joinPropertyMeta.setType(PropertyType.JOIN_SIMPLE);
		joinPropertyMeta.setSingleKey(true);

		joinPropertyMeta.setValueClass(UserBean.class);
		joinPropertyMeta.setJoinProperties(joinProperties);
		joinPropertyMeta.setSetter(userSetter);

		List<Pair<DynamicComposite, String>> columns = mock(List.class);
		when(columns.size()).thenReturn(1);

		DynamicComposite comp = new DynamicComposite();
		comp.addComponent(0, 0, ComponentEquality.EQUAL);
		comp.addComponent(1, 0, ComponentEquality.EQUAL);

		when(keyFactory.createBaseForQuery(joinPropertyMeta, EQUAL)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn(null);

		loader.loadPropertyIntoObject(bean, id, dao, joinPropertyMeta);

		assertThat(bean.getUser()).isNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_null_when_join_entity_not_found() throws Exception
	{
		Long joinId = 45L;

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(dao);

		List<Pair<DynamicComposite, String>> columns = mock(List.class);

		when(dao.eagerFetchEntity(joinId)).thenReturn(columns);
		when(columns.size()).thenReturn(0);

		UserBean loadedBean = loader.loadJoinEntity(UserBean.class, joinId, entityMeta);

		assertThat(loadedBean).isNull();

	}

	private String writeToString(Object object) throws Exception
	{
		return objectMapper.writeValueAsString(object);
	}
}
