package info.archinnov.achilles.entity.operations;

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
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.EntityMapper;
import info.archinnov.achilles.entity.manager.CompleteBeanTestBuilder;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.KeyValue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

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
	private EntityMeta<Long> joinMeta;

	@Mock
	private PropertyMeta<Void, Long> joinIdMeta;

	@Mock
	private EntityMapper mapper;

	@Mock
	private GenericDynamicCompositeDao<Long> dao;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	@Mock
	private EntityHelper helper;

	@Mock
	private JoinEntityLoader joinLoader;

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
		Whitebox.setInternalState(loader, "helper", helper);
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
	public void should_load_column_family() throws Exception
	{
		PropertyMeta<Void, Long> idMeta = mock(PropertyMeta.class);
		Method idSetter = ColumnFamilyBean.class.getDeclaredMethod("setId", Long.class);

		when(entityMeta.isColumnFamilyDirectMapping()).thenReturn(true);
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
	public void should_load_version_serial_uid() throws Exception
	{
		when(dao.getValue(eq(1L), any(DynamicComposite.class))).thenReturn("12345");
		Long versionSerialUID = loader.loadVersionSerialUID(1L, dao);

		assertThat(versionSerialUID).isEqualTo(12345L);
	}

	@Test
	public void should_return_null_when_no_serial_version_uid_found() throws Exception
	{
		Long versionSerialUID = loader.loadVersionSerialUID(1L, dao);

		assertThat(versionSerialUID).isNull();
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
	public void should_load_join_simple() throws Exception
	{
		Long key = 11L;
		Method idSetter = UserBean.class.getDeclaredMethod("setUserId", Long.class);

		PropertyMeta<Void, UserBean> propertyMeta = mock(PropertyMeta.class);

		when(propertyMeta.type()).thenReturn(PropertyType.JOIN_SIMPLE);
		when(propertyMeta.getSetter()).thenReturn(idSetter);

		when(propertyMeta.getValueClass()).thenReturn(UserBean.class);
		when((EntityMeta<Long>) propertyMeta.joinMeta()).thenReturn(joinMeta);
		when((PropertyMeta<Void, Long>) propertyMeta.joinIdMeta()).thenReturn(joinIdMeta);

		DynamicComposite comp = new DynamicComposite();
		when(keyFactory.createBaseForQuery(propertyMeta, EQUAL)).thenReturn(comp);

		when(dao.getValue(key, comp)).thenReturn("120");
		when(joinIdMeta.getValueFromString("120")).thenReturn(120L);
		when(joinMeta.getEntityDao()).thenReturn(dao);
		when(joinMeta.getIdMeta()).thenReturn(joinIdMeta);
		when(joinIdMeta.getSetter()).thenReturn(idSetter);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();
		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();
		columns.add(new Pair<DynamicComposite, String>(start, "John"));
		columns.add(new Pair<DynamicComposite, String>(end, "DOE"));

		when(dao.eagerFetchEntity(120L)).thenReturn(columns);

		ArgumentCaptor<UserBean> userCaptor = ArgumentCaptor.forClass(UserBean.class);

		CompleteBean realObject = new CompleteBean();
		loader.loadPropertyIntoObject(realObject, key, dao, propertyMeta);

		verify(mapper).setEagerPropertiesToEntity(eq(120L), eq(columns), eq(joinMeta),
				userCaptor.capture());
		verify(helper).setValueToField(userCaptor.capture(), eq(idSetter), eq(120L));

		verify(helper).setValueToField(eq(realObject), eq(idSetter), userCaptor.capture());

		List<UserBean> capturedUsers = userCaptor.getAllValues();
		assertThat(capturedUsers).hasSize(3);
		UserBean user = capturedUsers.get(0);
		assertThat(capturedUsers).containsExactly(user, user, user);
	}

	@Test
	public void should_load_join_list_into_entity() throws Exception
	{
		Method setter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		PropertyMeta<Void, UserBean> propertyMeta = new PropertyMeta<Void, UserBean>();
		propertyMeta.setType(PropertyType.JOIN_LIST);
		propertyMeta.setSetter(setter);

		CompleteBean realObject = new CompleteBean();
		List<UserBean> users = new ArrayList<UserBean>();
		when(joinLoader.loadJoinListProperty(11L, dao, propertyMeta)).thenReturn(users);

		this.loader.loadPropertyIntoObject(realObject, 11L, dao, propertyMeta);

		verify(helper).setValueToField(realObject, setter, users);
	}

	@Test
	public void should_load_join_set_into_entity() throws Exception
	{
		Method setter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		PropertyMeta<Void, UserBean> propertyMeta = new PropertyMeta<Void, UserBean>();
		propertyMeta.setType(PropertyType.JOIN_SET);
		propertyMeta.setSetter(setter);

		CompleteBean realObject = new CompleteBean();
		Set<UserBean> users = new HashSet<UserBean>();
		when(joinLoader.loadJoinSetProperty(11L, dao, propertyMeta)).thenReturn(users);

		this.loader.loadPropertyIntoObject(realObject, 11L, dao, propertyMeta);

		verify(helper).setValueToField(realObject, setter, users);
	}

	@Test
	public void should_load_join_map_into_entity() throws Exception
	{
		Method setter = CompleteBean.class.getDeclaredMethod("setId", Long.class);

		PropertyMeta<Integer, UserBean> propertyMeta = new PropertyMeta<Integer, UserBean>();
		propertyMeta.setType(PropertyType.JOIN_MAP);
		propertyMeta.setSetter(setter);

		CompleteBean realObject = new CompleteBean();
		Map<Integer, UserBean> users = new HashMap<Integer, UserBean>();
		when(joinLoader.loadJoinMapProperty(11L, dao, propertyMeta)).thenReturn(users);

		this.loader.loadPropertyIntoObject(realObject, 11L, dao, propertyMeta);

		verify(helper).setValueToField(realObject, setter, users);
	}

	private String writeToString(Object object) throws Exception
	{
		return objectMapper.writeValueAsString(object);
	}
}
