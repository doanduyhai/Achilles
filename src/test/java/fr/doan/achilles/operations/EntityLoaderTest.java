package fr.doan.achilles.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import fr.doan.achilles.bean.BeanMapper;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;

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
	private PropertyMeta<String> propertyMeta;

	@Mock
	private ListPropertyMeta<String> listPropertyMeta;

	@Mock
	private SetPropertyMeta<String> setPropertyMeta;

	@Mock
	private MapPropertyMeta<String> mapPropertyMeta;

	@Mock
	private BeanMapper mapper;

	@Mock
	private GenericDao<Long> dao;

	@Test
	public void should_load_entity() throws Exception
	{
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
		columns.add(new Pair<Composite, Object>(new Composite(), ""));

		when(entityMeta.getDao()).thenReturn(dao);
		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		loader.load(CompleteBean.class, 1L, entityMeta);

		verify(mapper).mapColumnsToBean(eq(1L), eq(columns), eq(entityMeta), any(CompleteBean.class));
	}

	@Test
	public void should_not_load_entity_because_not_found() throws Exception
	{
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();

		when(entityMeta.getDao()).thenReturn(dao);
		when(dao.eagerFetchEntity(1L)).thenReturn(columns);
		CompleteBean bean = loader.load(CompleteBean.class, 1L, entityMeta);

		assertThat(bean).isNull();
		verifyZeroInteractions(mapper);
	}

	@Test(expected = RuntimeException.class)
	public void should_exception_when_error() throws Exception
	{

		when(entityMeta.getDao()).thenThrow(new RuntimeException());
		loader.load(CompleteBean.class, 1L, entityMeta);
	}

	@Test
	public void should_load_simple_property() throws Exception
	{
		Composite composite = new Composite();
		when(dao.buildCompositeForProperty("name", PropertyType.SIMPLE, 0)).thenReturn(composite);
		when(dao.getValue(1L, composite)).thenReturn("name");
		when(propertyMeta.get("name")).thenReturn("name");

		String value = (String) loader.loadSimpleProperty(1L, "name", dao, propertyMeta);
		assertThat(value).isEqualTo("name");
	}

	@Test
	public void should_load_list_property() throws Exception
	{
		Composite start = new Composite();
		Composite end = new Composite();

		List<Pair<Composite, Object>> friends = new ArrayList<Pair<Composite, Object>>();
		friends.add(new Pair<Composite, Object>(start, "foo"));
		friends.add(new Pair<Composite, Object>(end, "bar"));

		when(dao.buildCompositeComparatorStart("friends", PropertyType.LIST)).thenReturn(start);
		when(dao.buildCompositeComparatorEnd("friends", PropertyType.LIST)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(friends);

		when(listPropertyMeta.newListInstance()).thenReturn(new ArrayList<String>());
		when((String) listPropertyMeta.get("foo")).thenReturn("foo");
		when((String) listPropertyMeta.get("bar")).thenReturn("bar");

		List<String> value = loader.loadListProperty(1L, "friends", dao, listPropertyMeta);

		assertThat(value).hasSize(2);
		assertThat(value).containsExactly("foo", "bar");
	}

	@Test
	public void should_load_set_property() throws Exception
	{
		Composite start = new Composite();
		Composite end = new Composite();

		List<Pair<Composite, Object>> followers = new ArrayList<Pair<Composite, Object>>();
		followers.add(new Pair<Composite, Object>(start, "George"));
		followers.add(new Pair<Composite, Object>(end, "Paul"));

		when(dao.buildCompositeComparatorStart("followers", PropertyType.SET)).thenReturn(start);
		when(dao.buildCompositeComparatorEnd("followers", PropertyType.SET)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(followers);

		when(setPropertyMeta.newSetInstance()).thenReturn(new HashSet<String>());
		when((String) setPropertyMeta.get("George")).thenReturn("George");
		when((String) setPropertyMeta.get("Paul")).thenReturn("Paul");

		Set<String> value = loader.loadSetProperty(1L, "followers", dao, setPropertyMeta);

		assertThat(value).hasSize(2);
		assertThat(value).contains("George", "Paul");
	}

	@Test
	public void should_load_map_property() throws Exception
	{
		Composite start = new Composite();
		Composite middle = new Composite();
		Composite end = new Composite();

		List<Pair<Composite, Object>> preferences = new ArrayList<Pair<Composite, Object>>();
		preferences.add(new Pair<Composite, Object>(start, new KeyValueHolder(1, "FR")));
		preferences.add(new Pair<Composite, Object>(middle, new KeyValueHolder(2, "Paris")));
		preferences.add(new Pair<Composite, Object>(end, new KeyValueHolder(3, "75014")));

		when(dao.buildCompositeComparatorStart("preferences", PropertyType.MAP)).thenReturn(start);
		when(dao.buildCompositeComparatorEnd("preferences", PropertyType.MAP)).thenReturn(end);
		when(dao.findColumnsRange(1L, start, end, false, Integer.MAX_VALUE)).thenReturn(preferences);

		when(mapPropertyMeta.getKeyClass()).thenReturn(Integer.class);

		when((String) mapPropertyMeta.get("FR")).thenReturn("FR");
		when((String) mapPropertyMeta.get("Paris")).thenReturn("Paris");
		when((String) mapPropertyMeta.get("75014")).thenReturn("75014");

		Map<Serializable, String> value = loader.loadMapProperty(1L, "preferences", dao, mapPropertyMeta);

		assertThat(value).hasSize(3);
		assertThat(value).containsKey(1);
		assertThat(value).containsValue("FR");
		assertThat(value).containsKey(2);
		assertThat(value).containsValue("Paris");
		assertThat(value).containsKey(3);
		assertThat(value).containsValue("75014");
	}
}
