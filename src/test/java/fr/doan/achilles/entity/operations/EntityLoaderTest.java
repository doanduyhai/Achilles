package fr.doan.achilles.entity.operations;

import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SET;
import static fr.doan.achilles.entity.metadata.PropertyType.LAZY_SIMPLE;
import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import mapping.entity.UserBean;
import mapping.entity.WideRowBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.EntityMapper;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.JoinProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.proxy.builder.EntityProxyBuilder;

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
	private GenericEntityDao<Long> dao;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private EntityProxyBuilder interceptorBuilder;

	@Mock
	private EntityHelper helper;

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
		List<Pair<DynamicComposite, Object>> columns = new ArrayList<Pair<DynamicComposite, Object>>();
		columns.add(new Pair<DynamicComposite, Object>(new DynamicComposite(), ""));
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		Method idSetter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getSetter()).thenReturn(idSetter);

		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		doNothing().when(helper).setValueToField(any(CompleteBean.class), eq(idSetter),
				idCaptor.capture());

		loader.load(CompleteBean.class, 1L, entityMeta);

		verify(mapper).mapColumnsToBean(eq(1L), eq(columns), eq(entityMeta),
				any(CompleteBean.class));

		assertThat(idCaptor.getValue()).isEqualTo(1L);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_load_widerow() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		Method idSetter = WideRowBean.class.getDeclaredMethod("setId", Long.class);

		when(entityMeta.isWideRow()).thenReturn(true);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.getSetter()).thenReturn(idSetter);
		doNothing().when(helper).setValueToField(any(CompleteBean.class), eq(idSetter),
				idCaptor.capture());

		loader.load(WideRowBean.class, 452L, entityMeta);

		assertThat(idCaptor.getValue()).isEqualTo(452L);
	}

	@Test
	public void should_not_load_entity_because_not_found() throws Exception
	{
		List<Pair<DynamicComposite, Object>> columns = new ArrayList<Pair<DynamicComposite, Object>>();

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
		when(propertyMeta.getValue("name")).thenReturn("name");
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

		List<Pair<DynamicComposite, Object>> friends = new ArrayList<Pair<DynamicComposite, Object>>();
		friends.add(new Pair<DynamicComposite, Object>(start, "foo"));
		friends.add(new Pair<DynamicComposite, Object>(end, "bar"));

		when(keyFactory.createBaseForQuery(listMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(listMeta, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(friends);

		when(listMeta.newListInstance()).thenReturn(new ArrayList<String>());
		when(listMeta.getValue("foo")).thenReturn("foo");
		when(listMeta.getValue("bar")).thenReturn("bar");

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

		List<Pair<DynamicComposite, Object>> followers = new ArrayList<Pair<DynamicComposite, Object>>();
		followers.add(new Pair<DynamicComposite, Object>(start, "George"));
		followers.add(new Pair<DynamicComposite, Object>(end, "Paul"));

		when(keyFactory.createBaseForQuery(setMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(setMeta, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(followers);

		when(setMeta.newSetInstance()).thenReturn(new HashSet<String>());
		when(setMeta.getValue("George")).thenReturn("George");
		when(setMeta.getValue("Paul")).thenReturn("Paul");

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

		List<Pair<DynamicComposite, Object>> preferences = new ArrayList<Pair<DynamicComposite, Object>>();
		preferences.add(new Pair<DynamicComposite, Object>(start, new KeyValueHolder(1, "FR")));
		preferences.add(new Pair<DynamicComposite, Object>(middle, new KeyValueHolder(2, "Paris")));
		preferences.add(new Pair<DynamicComposite, Object>(end, new KeyValueHolder(3, "75014")));

		when(keyFactory.createBaseForQuery(mapMeta, EQUAL)).thenReturn(start);
		when(keyFactory.createBaseForQuery(mapMeta, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE))
				.thenReturn(preferences);

		when(mapMeta.getKeyClass()).thenReturn(Integer.class);

		when(mapMeta.getValue("FR")).thenReturn("FR");
		when(mapMeta.getValue("Paris")).thenReturn("Paris");
		when(mapMeta.getValue("75014")).thenReturn("75014");

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
		joinPropertyMeta.setLazy(true);
		joinPropertyMeta.setJoinColumn(true);

		joinPropertyMeta.setValueClass(UserBean.class);
		joinPropertyMeta.setJoinProperties(joinProperties);

		UserBean userBean = new UserBean();

		List<Pair<DynamicComposite, Object>> columns = mock(List.class);
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
		joinPropertyMeta.setLazy(true);
		joinPropertyMeta.setJoinColumn(true);

		joinPropertyMeta.setValueClass(UserBean.class);
		joinPropertyMeta.setJoinProperties(joinProperties);
		joinPropertyMeta.setSetter(userSetter);

		UserBean userBean = new UserBean();
		userBean.setUserId(joinId);

		List<Pair<DynamicComposite, Object>> columns = mock(List.class);
		when(columns.size()).thenReturn(1);

		DynamicComposite comp = new DynamicComposite();
		comp.addComponent(0, 0, ComponentEquality.EQUAL);
		comp.addComponent(1, 0, ComponentEquality.EQUAL);

		when(keyFactory.createBaseForQuery(joinPropertyMeta, EQUAL)).thenReturn(comp);
		when(dao.getValue(id, comp)).thenReturn(joinId);
		when(dao.eagerFetchEntity(joinId)).thenReturn(columns);
		doNothing().when(helper).setValueToField(any(UserBean.class), eq(idSetter),
				idCaptor.capture());
		when(interceptorBuilder.build(userBeanCaptor.capture(), eq(entityMeta))).thenReturn(
				userBean);
		doNothing().when(helper)
				.setValueToField(eq(bean), eq(userSetter), userBeanCaptor.capture());

		loader.loadPropertyIntoObject(bean, id, dao, joinPropertyMeta);

		assertThat(userBeanCaptor.getValue()).isSameAs(userBean);
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
		joinPropertyMeta.setLazy(true);
		joinPropertyMeta.setJoinColumn(true);

		joinPropertyMeta.setValueClass(UserBean.class);
		joinPropertyMeta.setJoinProperties(joinProperties);
		joinPropertyMeta.setSetter(userSetter);

		List<Pair<DynamicComposite, Object>> columns = mock(List.class);
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
	@Test(expected = ValidationException.class)
	public void should_exception_when_join_entity_not_found() throws Exception
	{
		Long joinId = 45L;

		EntityMeta<Long> entityMeta = new EntityMeta<Long>();
		entityMeta.setEntityDao(dao);

		List<Pair<DynamicComposite, Object>> columns = mock(List.class);

		when(dao.eagerFetchEntity(joinId)).thenReturn(columns);
		when(columns.size()).thenReturn(0);

		loader.loadJoinEntity(UserBean.class, joinId, entityMeta);

	}
}
