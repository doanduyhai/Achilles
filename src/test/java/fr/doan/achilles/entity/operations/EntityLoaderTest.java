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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.EntityMapper;
import fr.doan.achilles.entity.manager.CompleteBeanTestBuilder;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.entity.type.KeyValueHolder;
import fr.doan.achilles.wrapper.factory.DynamicCompositeKeyFactory;

@RunWith(MockitoJUnitRunner.class)
public class EntityLoaderTest
{

	@InjectMocks
	private EntityLoader loader = new EntityLoader();

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private EntityMeta<Long> entityMeta;

	@Mock
	private PropertyMeta<?, String> propertyMeta;

	@Mock
	private ListMeta<String> listMeta;

	@Mock
	private SetMeta<String> setMeta;

	@Mock
	private MapMeta<Integer, String> mapMeta;

	@Mock
	private EntityMapper mapper;

	@Mock
	private GenericEntityDao<Long> dao;

	@Mock
	private DynamicCompositeKeyFactory keyFactory;

	private CompleteBean bean;

	@Before
	public void setUp()
	{
		bean = CompleteBeanTestBuilder.builder().buid();
	}

	@Test
	public void should_load_entity() throws Exception
	{
		List<Pair<DynamicComposite, Object>> columns = new ArrayList<Pair<DynamicComposite, Object>>();
		columns.add(new Pair<DynamicComposite, Object>(new DynamicComposite(), ""));

		when(entityMeta.getEntityDao()).thenReturn(dao);
		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		loader.load(CompleteBean.class, 1L, entityMeta);

		verify(mapper).mapColumnsToBean(eq(1L), eq(columns), eq(entityMeta),
				any(CompleteBean.class));
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
		when(propertyMeta.propertyType()).thenReturn(SIMPLE);
		DynamicComposite composite = new DynamicComposite();
		when(keyFactory.buildForInsert("name", SIMPLE, 0)).thenReturn(composite);
		when(dao.getValue(1L, composite)).thenReturn("name");

		String value = (String) loader.loadSimpleProperty(1L, dao, propertyMeta);
		assertThat(value).isEqualTo("name");
	}

	@Test
	public void should_load_list_property() throws Exception
	{
		when(listMeta.getPropertyName()).thenReturn("friends");
		when(listMeta.propertyType()).thenReturn(LAZY_LIST);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		List<Pair<DynamicComposite, Object>> friends = new ArrayList<Pair<DynamicComposite, Object>>();
		friends.add(new Pair<DynamicComposite, Object>(start, "foo"));
		friends.add(new Pair<DynamicComposite, Object>(end, "bar"));

		when(keyFactory.buildQueryComparator("friends", LAZY_LIST, EQUAL)).thenReturn(start);
		when(keyFactory.buildQueryComparator("friends", LAZY_LIST, GREATER_THAN_EQUAL)).thenReturn(
				end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(friends);

		when(listMeta.newListInstance()).thenReturn(new ArrayList<String>());
		when((String) listMeta.getValue("foo")).thenReturn("foo");
		when((String) listMeta.getValue("bar")).thenReturn("bar");

		List<String> value = loader.loadListProperty(1L, dao, listMeta);

		assertThat(value).hasSize(2);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_load_set_property() throws Exception
	{
		when(setMeta.getPropertyName()).thenReturn("followers");
		when(setMeta.propertyType()).thenReturn(SET);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		List<Pair<DynamicComposite, Object>> followers = new ArrayList<Pair<DynamicComposite, Object>>();
		followers.add(new Pair<DynamicComposite, Object>(start, "George"));
		followers.add(new Pair<DynamicComposite, Object>(end, "Paul"));

		when(keyFactory.buildQueryComparator("followers", SET, EQUAL)).thenReturn(start);
		when(keyFactory.buildQueryComparator("followers", SET, GREATER_THAN_EQUAL)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(followers);

		when(setMeta.newSetInstance()).thenReturn(new HashSet<String>());
		when((String) setMeta.getValue("George")).thenReturn("George");
		when((String) setMeta.getValue("Paul")).thenReturn("Paul");

		Set<String> value = loader.loadSetProperty(1L, dao, setMeta);

		assertThat(value).hasSize(2);
		assertThat(value).contains("George", "Paul");
	}

	@Test
	public void should_load_map_property() throws Exception
	{
		when(mapMeta.getPropertyName()).thenReturn("preferences");
		when(mapMeta.propertyType()).thenReturn(LAZY_MAP);

		DynamicComposite start = new DynamicComposite();
		DynamicComposite middle = new DynamicComposite();
		DynamicComposite end = new DynamicComposite();

		List<Pair<DynamicComposite, Object>> preferences = new ArrayList<Pair<DynamicComposite, Object>>();
		preferences.add(new Pair<DynamicComposite, Object>(start, new KeyValueHolder(1, "FR")));
		preferences.add(new Pair<DynamicComposite, Object>(middle, new KeyValueHolder(2, "Paris")));
		preferences.add(new Pair<DynamicComposite, Object>(end, new KeyValueHolder(3, "75014")));

		when(keyFactory.buildQueryComparator("preferences", LAZY_MAP, EQUAL)).thenReturn(start);
		when(keyFactory.buildQueryComparator("preferences", LAZY_MAP, GREATER_THAN_EQUAL))
				.thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE))
				.thenReturn(preferences);

		when(mapMeta.getKeyClass()).thenReturn(Integer.class);

		when((String) mapMeta.getValue("FR")).thenReturn("FR");
		when((String) mapMeta.getValue("Paris")).thenReturn("Paris");
		when((String) mapMeta.getValue("75014")).thenReturn("75014");

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
		when(propertyMeta.propertyType()).thenReturn(SIMPLE);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, propertyMeta);

		verify(spy).loadSimpleProperty(1L, dao, propertyMeta);
		verify(propertyMeta).getSetter();
	}

	@Test
	public void should_load_simple_lazy_property_into_object() throws Exception
	{
		when(propertyMeta.propertyType()).thenReturn(LAZY_SIMPLE);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, propertyMeta);

		verify(spy).loadSimpleProperty(1L, dao, propertyMeta);
		verify(propertyMeta).getSetter();
	}

	@Test
	public void should_load_list_property_into_object() throws Exception
	{
		when(listMeta.propertyType()).thenReturn(LIST);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, listMeta);

		verify(spy).loadListProperty(1L, dao, listMeta);
		verify(listMeta).getSetter();
	}

	@Test
	public void should_load_list_lazy_property_into_object() throws Exception
	{
		when(listMeta.propertyType()).thenReturn(LAZY_LIST);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, listMeta);

		verify(spy).loadListProperty(1L, dao, listMeta);
		verify(listMeta).getSetter();
	}

	@Test
	public void should_load_set_property_into_object() throws Exception
	{
		when(setMeta.propertyType()).thenReturn(SET);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, setMeta);

		verify(spy).loadSetProperty(1L, dao, setMeta);
		verify(setMeta).getSetter();
	}

	@Test
	public void should_load_set_lazy_property_into_object() throws Exception
	{
		when(setMeta.propertyType()).thenReturn(LAZY_SET);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, setMeta);

		verify(spy).loadSetProperty(1L, dao, setMeta);
		verify(setMeta).getSetter();
	}

	@Test
	public void should_load_map_property_into_object() throws Exception
	{
		when(mapMeta.propertyType()).thenReturn(MAP);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, mapMeta);

		verify(spy).loadMapProperty(1L, dao, mapMeta);
		verify(mapMeta).getSetter();
	}

	@Test
	public void should_load_map_lazy_property_into_object() throws Exception
	{
		when(mapMeta.propertyType()).thenReturn(LAZY_MAP);
		EntityLoader spy = spy(loader);
		spy.loadPropertyIntoObject(bean, 1L, dao, mapMeta);

		verify(spy).loadMapProperty(1L, dao, mapMeta);
		verify(mapMeta).getSetter();
	}
}
