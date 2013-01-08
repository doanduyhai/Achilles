package fr.doan.achilles.entity;

import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import fr.doan.achilles.columnFamily.ColumnFamilyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.parser.EntityParser;
import fr.doan.achilles.holder.KeyValueHolder;

@SuppressWarnings(
{
		"unchecked",
		"rawtypes"
})
@RunWith(MockitoJUnitRunner.class)
public class EntityMapperTest
{
	@InjectMocks
	private EntityMapper mapper;

	@Mock
	private EntityHelper helper;

	@Mock
	private ExecutingKeyspace keyspace;

	@Mock
	private Map<Class<?>, EntityMeta<?>> entityMetaMap;

	@Mock
	private Map<PropertyMeta<?, ?>, Class<?>> joinPropertyMetaToBeFilled;

	@Mock
	private ColumnFamilyHelper columnFamilyHelper;

	@Captor
	ArgumentCaptor<Long> idCaptor;

	@Captor
	ArgumentCaptor<String> simpleCaptor;

	@Captor
	ArgumentCaptor<List> listCaptor;

	@Captor
	ArgumentCaptor<Set> setCaptor;

	@Captor
	ArgumentCaptor<Map> mapCaptor;

	private EntityParser parser = new EntityParser();

	private EntityMeta<Long> entityMeta;

	@Before
	public void setUp()
	{
		ReflectionTestUtils.setField(mapper, "helper", helper);

		entityMeta = (EntityMeta<Long>) parser.parseEntity(keyspace, CompleteBean.class,
				joinPropertyMetaToBeFilled);
	}

	@Test
	public void should_map_id_property() throws Exception
	{

		CompleteBean entity = new CompleteBean();
		doNothing().when(helper).setValueToField(eq(entity),
				eq(entityMeta.getIdMeta().getSetter()), idCaptor.capture());

		mapper.mapIdToBean(1L, entityMeta.getIdMeta(), entity);

		assertThat(idCaptor.getValue()).isEqualTo(1L);
	}

	@Test
	public void should_map_simple_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		PropertyMeta<?, ?> namePropertyMeta = entityMeta.getPropertyMetas().get("name");

		doNothing().when(helper).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()),
				simpleCaptor.capture());

		mapper.mapSimplePropertyToBean("name", namePropertyMeta, entity);

		assertThat(simpleCaptor.getValue()).isEqualTo("name");
	}

	@Test
	public void should_map_list_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		PropertyMeta<?, ?> listPropertyMeta = entityMeta.getPropertyMetas().get("friends");

		doNothing().when(helper).setValueToField(eq(entity), eq(listPropertyMeta.getSetter()),
				listCaptor.capture());

		mapper.mapListPropertyToBean(Arrays.asList("foo", "bar"), listPropertyMeta, entity);

		assertThat(listCaptor.getValue()).hasSize(2);
		assertThat(listCaptor.getValue()).containsExactly("foo", "bar");
	}

	@Test
	public void should_map_set_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		PropertyMeta<?, ?> setPropertyMeta = entityMeta.getPropertyMetas().get("followers");

		doNothing().when(helper).setValueToField(eq(entity), eq(setPropertyMeta.getSetter()),
				setCaptor.capture());

		mapper.mapSetPropertyToBean(Sets.newHashSet("George", "Paul"), setPropertyMeta, entity);

		assertThat(setCaptor.getValue()).hasSize(2);
		assertThat(setCaptor.getValue()).contains("George", "Paul");
	}

	@Test
	public void should_map_map_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		Map<Integer, String> preferences = new HashMap<Integer, String>();

		preferences.put(1, "FR");
		preferences.put(2, "Paris");
		preferences.put(3, "75014");

		PropertyMeta<?, ?> mapPropertyMeta = entityMeta.getPropertyMetas().get("preferences");

		doNothing().when(helper).setValueToField(eq(entity), eq(mapPropertyMeta.getSetter()),
				mapCaptor.capture());

		mapper.mapMapPropertyToBean(preferences, mapPropertyMeta, entity);

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	@Test
	public void should_add_to_empty_list() throws Exception
	{

		Map<String, List> listProperties = new HashMap<String, List>();
		PropertyMeta<Void, ?> listMeta = (PropertyMeta<Void, ?>) entityMeta.getPropertyMetas().get(
				"friends");
		mapper.addToList(listProperties, listMeta, "foo");

		assertThat(listProperties).hasSize(1);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("foo");
	}

	@Test
	public void should_add_to_not_empty_list() throws Exception
	{

		Map<String, List> listProperties = new HashMap<String, List>();
		listProperties.put("test", Arrays.asList("test1", "test2"));
		PropertyMeta<Void, ?> listMeta = (PropertyMeta<Void, ?>) entityMeta.getPropertyMetas().get(
				"friends");
		mapper.addToList(listProperties, listMeta, "foo");

		assertThat(listProperties).hasSize(2);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("foo");

		assertThat(listProperties).containsKey("test");
		assertThat(listProperties.get("test")).containsExactly("test1", "test2");
	}

	@Test
	public void should_add_to_empty_set() throws Exception
	{

		Map<String, Set> setProperties = new HashMap<String, Set>();
		PropertyMeta<Void, ?> setMeta = (PropertyMeta<Void, ?>) entityMeta.getPropertyMetas().get(
				"followers");
		mapper.addToSet(setProperties, setMeta, "George");

		assertThat(setProperties).hasSize(1);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsExactly("George");
	}

	@Test
	public void should_add_to_not_empty_set() throws Exception
	{

		Map<String, Set> setProperties = new HashMap<String, Set>();
		HashSet<Object> set = Sets.newHashSet();
		set.addAll(Arrays.asList("test1", "test2"));
		setProperties.put("test", set);

		PropertyMeta<Void, ?> setMeta = (PropertyMeta<Void, ?>) entityMeta.getPropertyMetas().get(
				"followers");
		mapper.addToSet(setProperties, setMeta, "George");

		assertThat(setProperties).hasSize(2);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsExactly("George");

		assertThat(setProperties).containsKey("test");
		assertThat(setProperties.get("test")).containsExactly("test1", "test2");
	}

	@Test
	public void should_add_to_empty_map() throws Exception
	{

		Map<String, Map> mapProperties = new HashMap<String, Map>();
		PropertyMeta<?, ?> mapMeta = entityMeta.getPropertyMetas().get("preferences");
		mapper.addToMap(mapProperties, mapMeta, new KeyValueHolder(1, "FR"));

		assertThat(mapProperties).hasSize(1);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");
	}

	@Test
	public void should_add_to_not_empty_map() throws Exception
	{

		Map<String, Map> mapProperties = new HashMap<String, Map>();

		HashMap map = Maps.newHashMap();
		map.put(2, "Paris");
		map.put(3, "75014");
		mapProperties.put("test", map);

		PropertyMeta<?, ?> mapMeta = entityMeta.getPropertyMetas().get("preferences");
		mapper.addToMap(mapProperties, mapMeta, new KeyValueHolder(1, "FR"));

		assertThat(mapProperties).hasSize(2);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");

		assertThat(mapProperties).containsKey("test");
		assertThat(mapProperties.get("test").get(2)).isEqualTo("Paris");
		assertThat(mapProperties.get("test").get(3)).isEqualTo("75014");
	}

	@Test
	public void should_map_columns_to_bean() throws Exception
	{

		CompleteBean entity = new CompleteBean();

		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
		PropertyMeta<?, ?> simpleMeta = entityMeta.getPropertyMetas().get("name");
		PropertyMeta<Void, ?> listMeta = (PropertyMeta<Void, ?>) entityMeta.getPropertyMetas().get(
				"friends");
		PropertyMeta<Void, ?> setMeta = (PropertyMeta<Void, ?>) entityMeta.getPropertyMetas().get(
				"followers");
		PropertyMeta<?, ?> mapMeta = entityMeta.getPropertyMetas().get("preferences");

		List<Pair<DynamicComposite, Object>> columns = new ArrayList<Pair<DynamicComposite, Object>>();

		columns.add(new Pair<DynamicComposite, Object>(buildSimplePropertyComposite("name"), "name"));

		columns.add(new Pair<DynamicComposite, Object>(buildListPropertyComposite("friends"), "foo"));
		columns.add(new Pair<DynamicComposite, Object>(buildListPropertyComposite("friends"), "bar"));

		columns.add(new Pair<DynamicComposite, Object>(buildSetPropertyComposite("followers"),
				"George"));
		columns.add(new Pair<DynamicComposite, Object>(buildSetPropertyComposite("followers"),
				"Paul"));

		columns.add(new Pair<DynamicComposite, Object>(buildMapPropertyComposite("preferences"),
				new KeyValueHolder(1, "FR")));
		columns.add(new Pair<DynamicComposite, Object>(buildMapPropertyComposite("preferences"),
				new KeyValueHolder(2, "Paris")));
		columns.add(new Pair<DynamicComposite, Object>(buildMapPropertyComposite("preferences"),
				new KeyValueHolder(3, "75014")));

		doNothing().when(helper).setValueToField(eq(entity), eq(idMeta.getSetter()),
				idCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(simpleMeta.getSetter()),
				simpleCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(listMeta.getSetter()),
				listCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(setMeta.getSetter()),
				setCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(mapMeta.getSetter()),
				mapCaptor.capture());

		mapper.mapColumnsToBean(2L, columns, entityMeta, entity);

		assertThat(idCaptor.getValue()).isEqualTo(2L);
		assertThat(simpleCaptor.getValue()).isEqualTo("name");

		assertThat(listCaptor.getValue()).hasSize(2);
		assertThat(listCaptor.getValue()).containsExactly("foo", "bar");

		assertThat(setCaptor.getValue()).hasSize(2);
		assertThat(setCaptor.getValue()).contains("George", "Paul");

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	private DynamicComposite buildSimplePropertyComposite(String propertyName)
	{
		DynamicComposite comp = new DynamicComposite();
		comp.add(0, SIMPLE.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private DynamicComposite buildListPropertyComposite(String propertyName)
	{
		DynamicComposite comp = new DynamicComposite();
		comp.add(0, LIST.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private DynamicComposite buildSetPropertyComposite(String propertyName)
	{
		DynamicComposite comp = new DynamicComposite();
		comp.add(0, SET.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private DynamicComposite buildMapPropertyComposite(String propertyName)
	{
		DynamicComposite comp = new DynamicComposite();
		comp.add(0, MAP.flag());
		comp.add(1, propertyName);
		return comp;
	}
}
