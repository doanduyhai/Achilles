package fr.doan.achilles.entity;

import static fr.doan.achilles.entity.metadata.PropertyType.LIST;
import static fr.doan.achilles.entity.metadata.PropertyType.MAP;
import static fr.doan.achilles.entity.metadata.PropertyType.SET;
import static fr.doan.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapping.entity.CompleteBean;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListPropertyMeta;
import fr.doan.achilles.entity.metadata.MapPropertyMeta;
import fr.doan.achilles.entity.metadata.SetPropertyMeta;
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

	private EntityParser parser = new EntityParser();

	@Mock
	private ExecutingKeyspace keyspace;

	private EntityMeta<Long> entityMeta;

	private EntityMapper mapper = new EntityMapper();

	@Before
	public void setUp()
	{
		entityMeta = (EntityMeta<Long>) parser.parseEntity(keyspace, CompleteBean.class);
	}

	@Test
	public void should_map_id_property() throws Exception
	{

		CompleteBean entity = new CompleteBean();
		mapper.mapIdToBean(1L, entityMeta.getIdMeta(), entity);

		assertThat(entity.getId()).isEqualTo(1L);
	}

	@Test
	public void should_map_simple_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		mapper.mapSimplePropertyToBean("name", entityMeta.getPropertyMetas().get("name"), entity);

		assertThat(entity.getName()).isEqualTo("name");
	}

	@Test
	public void should_map_list_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		mapper.mapListPropertyToBean(Arrays.asList("foo", "bar"), entityMeta.getPropertyMetas().get("friends"), entity);

		assertThat(entity.getFriends()).hasSize(2);
		assertThat(entity.getFriends()).containsExactly("foo", "bar");
	}

	@Test
	public void should_map_set_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		mapper.mapSetPropertyToBean(Sets.newHashSet("George", "Paul"), entityMeta.getPropertyMetas().get("followers"), entity);

		assertThat(entity.getFollowers()).hasSize(2);
		assertThat(entity.getFollowers()).contains("George", "Paul");
	}

	@Test
	public void should_map_map_property() throws Exception
	{
		CompleteBean entity = new CompleteBean();
		Map<Integer, String> preferences = new HashMap<Integer, String>();

		preferences.put(1, "FR");
		preferences.put(2, "Paris");
		preferences.put(3, "75014");
		mapper.mapMapPropertyToBean(preferences, entityMeta.getPropertyMetas().get("preferences"), entity);

		assertThat(entity.getPreferences()).hasSize(3);
		assertThat(entity.getPreferences().get(1)).isEqualTo("FR");
		assertThat(entity.getPreferences().get(2)).isEqualTo("Paris");
		assertThat(entity.getPreferences().get(3)).isEqualTo("75014");
	}

	@Test
	public void should_add_to_empty_list() throws Exception
	{

		Map<String, List> listProperties = new HashMap<String, List>();
		ListPropertyMeta<?> listMeta = (ListPropertyMeta<?>) entityMeta.getPropertyMetas().get("friends");
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
		ListPropertyMeta<?> listMeta = (ListPropertyMeta<?>) entityMeta.getPropertyMetas().get("friends");
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
		SetPropertyMeta<?> setMeta = (SetPropertyMeta<?>) entityMeta.getPropertyMetas().get("followers");
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

		SetPropertyMeta<?> setMeta = (SetPropertyMeta<?>) entityMeta.getPropertyMetas().get("followers");
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
		MapPropertyMeta<?, ?> mapMeta = (MapPropertyMeta<?, ?>) entityMeta.getPropertyMetas().get("preferences");
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

		MapPropertyMeta<?, ?> mapMeta = (MapPropertyMeta<?, ?>) entityMeta.getPropertyMetas().get("preferences");
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

		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();

		columns.add(new Pair<Composite, Object>(buildSimplePropertyComposite("name"), "name"));

		columns.add(new Pair<Composite, Object>(buildListPropertyComposite("friends"), "foo"));
		columns.add(new Pair<Composite, Object>(buildListPropertyComposite("friends"), "bar"));

		columns.add(new Pair<Composite, Object>(buildSetPropertyComposite("followers"), "George"));
		columns.add(new Pair<Composite, Object>(buildSetPropertyComposite("followers"), "Paul"));

		columns.add(new Pair<Composite, Object>(buildMapPropertyComposite("preferences"), new KeyValueHolder(1, "FR")));
		columns.add(new Pair<Composite, Object>(buildMapPropertyComposite("preferences"), new KeyValueHolder(2, "Paris")));
		columns.add(new Pair<Composite, Object>(buildMapPropertyComposite("preferences"), new KeyValueHolder(3, "75014")));

		mapper.mapColumnsToBean(2L, columns, entityMeta, entity);

		assertThat(entity.getId()).isEqualTo(2L);
		assertThat(entity.getName()).isEqualTo("name");

		assertThat(entity.getFriends()).hasSize(2);
		assertThat(entity.getFriends()).containsExactly("foo", "bar");

		assertThat(entity.getFollowers()).hasSize(2);
		assertThat(entity.getFollowers()).contains("George", "Paul");

		assertThat(entity.getPreferences()).hasSize(3);
		assertThat(entity.getPreferences().get(1)).isEqualTo("FR");
		assertThat(entity.getPreferences().get(2)).isEqualTo("Paris");
		assertThat(entity.getPreferences().get(3)).isEqualTo("75014");
	}

	private Composite buildSimplePropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, SIMPLE.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private Composite buildListPropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, LIST.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private Composite buildSetPropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, SET.flag());
		comp.add(1, propertyName);
		return comp;
	}

	private Composite buildMapPropertyComposite(String propertyName)
	{
		Composite comp = new Composite();
		comp.add(0, MAP.flag());
		comp.add(1, propertyName);
		return comp;
	}
}
