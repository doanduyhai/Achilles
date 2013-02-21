package info.archinnov.achilles.entity;

import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SERIAL_VERSION_UID;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import info.archinnov.achilles.columnFamily.ColumnFamilyHelper;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parser.EntityParser;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.json.ObjectMapperFactory;

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

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * EntityMapperTest
 * 
 * @author DuyHai DOAN
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityMapperTest
{
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

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

	private ObjectMapper objectMapper = new ObjectMapper();

	@Captor
	ArgumentCaptor<Long> idCaptor;

	@Captor
	ArgumentCaptor<String> simpleCaptor;

	@Captor
	ArgumentCaptor<List<String>> listCaptor;

	@Captor
	ArgumentCaptor<Set<String>> setCaptor;

	@Captor
	ArgumentCaptor<Map<Integer, String>> mapCaptor;

	private EntityParser parser;

	private EntityMeta<Long> entityMeta;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp()
	{
		ObjectMapperFactory factory = new ObjectMapperFactory()
		{
			@Override
			public <T> ObjectMapper getMapper(Class<T> type)
			{
				return objectMapper;
			}
		};
		Whitebox.setInternalState(mapper, "helper", helper);
		parser = new EntityParser(factory);
		entityMeta = (EntityMeta<Long>) parser.parseEntity(keyspace, CompleteBean.class).left;
	}

	@Test
	public void should_map_id_property() throws Exception
	{

		CompleteBean entity = new CompleteBean();
		doNothing().when(helper).setValueToField(eq(entity),
				eq(entityMeta.getIdMeta().getSetter()), idCaptor.capture());

		mapper.setIdToEntity(1L, entityMeta.getIdMeta(), entity);

		assertThat(idCaptor.getValue()).isEqualTo(1L);
	}

	@Test
	public void should_map_simple_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		PropertyMeta<?, ?> namePropertyMeta = entityMeta.getPropertyMetas().get("name");

		doNothing().when(helper).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()),
				simpleCaptor.capture());

		mapper.setSimplePropertyToEntity("name", namePropertyMeta, entity);

		assertThat(simpleCaptor.getValue()).isEqualTo("name");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_map_list_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		PropertyMeta<Void, String> listPropertyMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("friends");

		doNothing().when(helper).setValueToField(eq(entity), eq(listPropertyMeta.getSetter()),
				listCaptor.capture());

		mapper.setListPropertyToEntity(Arrays.asList("foo", "bar"), listPropertyMeta, entity);

		assertThat(listCaptor.getValue()).hasSize(2);
		assertThat(listCaptor.getValue()).containsExactly("foo", "bar");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_map_set_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();

		PropertyMeta<Void, String> setPropertyMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("followers");

		doNothing().when(helper).setValueToField(eq(entity), eq(setPropertyMeta.getSetter()),
				setCaptor.capture());

		mapper.setSetPropertyToEntity(Sets.newHashSet("George", "Paul"), setPropertyMeta, entity);

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

		mapper.setMapPropertyToEntity(preferences, mapPropertyMeta, entity);

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_add_to_empty_list() throws Exception
	{

		Map<String, List<String>> listProperties = new HashMap<String, List<String>>();
		PropertyMeta<Void, String> listMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("friends");
		mapper.addToList((Map) listProperties, listMeta, "foo");

		assertThat(listProperties).hasSize(1);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("foo");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_add_to_not_empty_list() throws Exception
	{

		Map<String, List<String>> listProperties = new HashMap<String, List<String>>();
		listProperties.put("test", Arrays.asList("test1", "test2"));
		PropertyMeta<Void, String> listMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("friends");
		mapper.addToList((Map) listProperties, listMeta, "foo");

		assertThat(listProperties).hasSize(2);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("foo");

		assertThat(listProperties).containsKey("test");
		assertThat(listProperties.get("test")).containsExactly("test1", "test2");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_add_to_empty_set() throws Exception
	{

		Map<String, Set<String>> setProperties = new HashMap<String, Set<String>>();
		PropertyMeta<Void, String> setMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("followers");
		mapper.addToSet((Map) setProperties, setMeta, "George");

		assertThat(setProperties).hasSize(1);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsExactly("George");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_add_to_not_empty_set() throws Exception
	{

		Map<String, Set<String>> setProperties = new HashMap<String, Set<String>>();
		HashSet<String> set = Sets.newHashSet();
		set.addAll(Arrays.asList("test1", "test2"));
		setProperties.put("test", set);

		PropertyMeta<Void, String> setMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("followers");
		mapper.addToSet((Map) setProperties, setMeta, "George");

		assertThat(setProperties).hasSize(2);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsExactly("George");

		assertThat(setProperties).containsKey("test");
		assertThat(setProperties.get("test")).containsExactly("test1", "test2");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_add_to_empty_map() throws Exception
	{

		Map<String, Map<Integer, String>> mapProperties = new HashMap<String, Map<Integer, String>>();
		PropertyMeta<Integer, String> mapMeta = (PropertyMeta<Integer, String>) entityMeta
				.getPropertyMetas().get("preferences");
		mapper.addToMap((Map) mapProperties, mapMeta, new KeyValue(1, "FR"));

		assertThat(mapProperties).hasSize(1);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	@Test
	public void should_add_to_not_empty_map() throws Exception
	{

		Map<String, Map<Integer, String>> mapProperties = new HashMap<String, Map<Integer, String>>();

		HashMap<Integer, String> map = Maps.newHashMap();
		map.put(2, "Paris");
		map.put(3, "75014");
		mapProperties.put("test", map);

		PropertyMeta<?, ?> mapMeta = entityMeta.getPropertyMetas().get("preferences");
		mapper.addToMap((Map) mapProperties, mapMeta, new KeyValue(1, "FR"));

		assertThat(mapProperties).hasSize(2);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");

		assertThat(mapProperties).containsKey("test");
		assertThat(mapProperties.get("test").get(2)).isEqualTo("Paris");
		assertThat(mapProperties.get("test").get(3)).isEqualTo("75014");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_map_columns_to_bean() throws Exception
	{

		CompleteBean entity = new CompleteBean();

		PropertyMeta<Void, Long> idMeta = entityMeta.getIdMeta();
		PropertyMeta<Void, String> simpleMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("name");
		PropertyMeta<Void, String> setMeta = (PropertyMeta<Void, String>) entityMeta
				.getPropertyMetas().get("followers");
		PropertyMeta<Integer, String> mapMeta = (PropertyMeta<Integer, String>) entityMeta
				.getPropertyMetas().get("preferences");

		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();

		columns.add(new Pair<DynamicComposite, String>(buildSimplePropertyComposite("name"), "name"));

		columns.add(new Pair<DynamicComposite, String>(buildListPropertyComposite("friends"), "foo"));
		columns.add(new Pair<DynamicComposite, String>(buildListPropertyComposite("friends"), "bar"));

		columns.add(new Pair<DynamicComposite, String>(buildSetPropertyComposite("followers"),
				"George"));
		columns.add(new Pair<DynamicComposite, String>(buildSetPropertyComposite("followers"),
				"Paul"));

		columns.add(new Pair<DynamicComposite, String>(buildMapPropertyComposite("preferences"),
				writeToString(new KeyValue<Integer, String>(1, "FR"))));
		columns.add(new Pair<DynamicComposite, String>(buildMapPropertyComposite("preferences"),
				writeToString(new KeyValue<Integer, String>(2, "Paris"))));
		columns.add(new Pair<DynamicComposite, String>(buildMapPropertyComposite("preferences"),
				writeToString(new KeyValue<Integer, String>(3, "75014"))));

		doNothing().when(helper).setValueToField(eq(entity), eq(idMeta.getSetter()),
				idCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(simpleMeta.getSetter()),
				simpleCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(setMeta.getSetter()),
				setCaptor.capture());
		doNothing().when(helper).setValueToField(eq(entity), eq(mapMeta.getSetter()),
				mapCaptor.capture());

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		assertThat(idCaptor.getValue()).isEqualTo(2L);
		assertThat(simpleCaptor.getValue()).isEqualTo("name");

		assertThat(setCaptor.getValue()).hasSize(2);
		assertThat(setCaptor.getValue()).contains("George", "Paul");

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	@Test
	public void should_exception_when_serialVersionUID_changes() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		List<Pair<DynamicComposite, String>> columns = new ArrayList<Pair<DynamicComposite, String>>();

		columns.add(new Pair<DynamicComposite, String>(
				buildSimplePropertyComposite(SERIAL_VERSION_UID.name()), "123"));

		expectedException.expect(IllegalStateException.class);
		expectedException
				.expectMessage("Saved serialVersionUID does not match current serialVersionUID for entity '"
						+ CompleteBean.class.getCanonicalName() + "'");

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);
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

	private String writeToString(Object object) throws Exception
	{
		return objectMapper.writeValueAsString(object);
	}
}
