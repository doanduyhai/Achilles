/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.helper;

import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.EntityMetaTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.type.KeyValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class EntityMapperTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@InjectMocks
	private EntityMapper mapper;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private Map<Class<?>, EntityMeta> entityMetaMap;

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

	private EntityMeta entityMeta;

	private PropertyMeta idMeta;

	private Map<PropertyMeta, Class<?>> joinPropertyMetaToBeFilled = new HashMap<PropertyMeta, Class<?>>();

	@Before
	public void setUp() throws Exception {
		joinPropertyMetaToBeFilled.clear();

		idMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, Long.class).field("id")
				.build();
	}

	@Test
	public void should_map_id_property() throws Exception {
		entityMeta = EntityMetaTestBuilder.builder(idMeta).build();

		CompleteBean entity = new CompleteBean();
		doNothing().when(invoker).setValueToField(eq(entity),
				eq(entityMeta.getIdMeta().getSetter()), idCaptor.capture());

		mapper.setIdToEntity(1L, entityMeta.getIdMeta(), entity);

		assertThat(idCaptor.getValue()).isEqualTo(1L);
	}

	@Test
	public void should_map_simple_property() throws Exception {
		CompleteBean entity = new CompleteBean();
		PropertyMeta namePropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class).field("name")
				.accessors().build();

		doNothing().when(invoker).setValueToField(eq(entity),
				eq(namePropertyMeta.getSetter()), simpleCaptor.capture());

		mapper.setSimplePropertyToEntity("name", namePropertyMeta, entity);

		assertThat(simpleCaptor.getValue()).isEqualTo("name");
	}

	@Test
	public void should_map_list_property() throws Exception {
		CompleteBean entity = new CompleteBean();

		PropertyMeta listPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class)
				.field("friends").accessors().build();

		doNothing().when(invoker).setValueToField(eq(entity),
				eq(listPropertyMeta.getSetter()), listCaptor.capture());

		mapper.setListPropertyToEntity(Arrays.asList("foo", "bar"),
				listPropertyMeta, entity);

		assertThat(listCaptor.getValue()).hasSize(2);
		assertThat(listCaptor.getValue()).containsExactly("foo", "bar");
	}

	@Test
	public void should_map_set_property() throws Exception {
		CompleteBean entity = new CompleteBean();

		PropertyMeta setPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class)
				.field("followers").accessors().build();

		doNothing().when(invoker).setValueToField(eq(entity),
				eq(setPropertyMeta.getSetter()), setCaptor.capture());

		mapper.setSetPropertyToEntity(Sets.newHashSet("George", "Paul"),
				setPropertyMeta, entity);

		assertThat(setCaptor.getValue()).hasSize(2);
		assertThat(setCaptor.getValue()).contains("George", "Paul");
	}

	@Test
	public void should_map_map_property() throws Exception {
		CompleteBean entity = new CompleteBean();
		Map<Integer, String> preferences = new HashMap<Integer, String>();

		preferences.put(1, "FR");
		preferences.put(2, "Paris");
		preferences.put(3, "75014");

		PropertyMeta mapPropertyMeta = PropertyMetaTestBuilder //
				.of(CompleteBean.class, Integer.class, String.class)
				//
				.field("preferences")
				//
				.accessors()
				//
				.build();

		doNothing().when(invoker).setValueToField(eq(entity),
				eq(mapPropertyMeta.getSetter()), mapCaptor.capture());

		mapper.setMapPropertyToEntity(preferences, mapPropertyMeta, entity);

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	@Test
	public void should_add_to_empty_list() throws Exception {

		Map<String, List<Object>> listProperties = new HashMap<String, List<Object>>();

		PropertyMeta listPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class)
				.field("friends").accessors().build();

		mapper.addToList(listProperties, listPropertyMeta, "foo");

		assertThat(listProperties).hasSize(1);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("foo");
	}

	@Test
	public void should_add_to_not_empty_list() throws Exception {

		Map<String, List<Object>> listProperties = new HashMap<String, List<Object>>();
		listProperties.put("test", Arrays.<Object> asList("test1", "test2"));
		PropertyMeta listPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class)
				.field("friends").accessors().build();
		mapper.addToList(listProperties, listPropertyMeta, "foo");

		assertThat(listProperties).hasSize(2);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("foo");

		assertThat(listProperties).containsKey("test");
		assertThat(listProperties.get("test"))
				.containsExactly("test1", "test2");
	}

	@Test
	public void should_add_to_empty_set() throws Exception {

		Map<String, Set<Object>> setProperties = new HashMap<String, Set<Object>>();
		PropertyMeta setPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class)
				.field("followers").accessors().build();

		mapper.addToSet(setProperties, setPropertyMeta, "George");

		assertThat(setProperties).hasSize(1);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsExactly("George");
	}

	@Test
	public void should_add_to_not_empty_set() throws Exception {

		Map<String, Set<Object>> setProperties = new HashMap<String, Set<Object>>();
		Set<Object> set = Sets.newHashSet();
		set.addAll(Arrays.asList("test1", "test2"));
		setProperties.put("test", set);

		PropertyMeta setPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class)
				.field("followers").accessors().build();
		mapper.addToSet(setProperties, setPropertyMeta, "George");

		assertThat(setProperties).hasSize(2);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsExactly("George");

		assertThat(setProperties).containsKey("test");
		assertThat(setProperties.get("test")).containsExactly("test1", "test2");
	}

	@Test
	public void should_add_to_empty_map() throws Exception {

		Map<String, Map<Object, Object>> mapProperties = new HashMap<String, Map<Object, Object>>();
		PropertyMeta mapPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Integer.class, String.class)
				.field("preferences").type(MAP).mapper(objectMapper)
				.accessors().build();
		mapper.addToMap(mapProperties, mapPropertyMeta,
				new KeyValue<Integer, String>(1, "FR"));

		assertThat(mapProperties).hasSize(1);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");
	}

	@Test
	public void should_add_to_not_empty_map() throws Exception {

		Map<String, Map<Object, Object>> mapProperties = new HashMap<String, Map<Object, Object>>();

		HashMap<Object, Object> map = Maps.newHashMap();
		map.put(2, "Paris");
		map.put(3, "75014");
		mapProperties.put("test", map);

		PropertyMeta mapPropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Integer.class, String.class)
				.field("preferences").type(MAP).mapper(objectMapper)
				.accessors().build();

		mapper.addToMap(mapProperties, mapPropertyMeta,
				new KeyValue<Integer, String>(1, "FR"));

		assertThat(mapProperties).hasSize(2);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");

		assertThat(mapProperties).containsKey("test");
		assertThat(mapProperties.get("test").get(2)).isEqualTo("Paris");
		assertThat(mapProperties.get("test").get(3)).isEqualTo("75014");
	}
}
