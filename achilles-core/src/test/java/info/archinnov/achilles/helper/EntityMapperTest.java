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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
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

	@Mock
	private DataTranscoder transcoder;

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

	@Test
	public void should_add_to_empty_list() throws Exception {

		Map<String, List<Object>> listProperties = new HashMap<String, List<Object>>();

		PropertyMeta listPropertyMeta = PropertyMetaTestBuilder
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
		List<Object> friends = new ArrayList<Object>();
		friends.add("test1");
		friends.add("test2");

		listProperties.put("friends", friends);
		PropertyMeta listPropertyMeta = PropertyMetaTestBuilder
				.of(CompleteBean.class, Void.class, String.class)
				.field("friends").accessors().build();
		mapper.addToList(listProperties, listPropertyMeta, "foo");

		assertThat(listProperties).hasSize(1);
		assertThat(listProperties).containsKey("friends");
		assertThat(listProperties.get("friends")).containsExactly("test1",
				"test2", "foo");
	}

	@Test
	public void should_add_to_empty_set() throws Exception {

		Map<String, Set<Object>> setProperties = new HashMap<String, Set<Object>>();
		PropertyMeta setPropertyMeta = PropertyMetaTestBuilder
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
		setProperties.put("followers", set);

		PropertyMeta setPropertyMeta = PropertyMetaTestBuilder
				.of(CompleteBean.class, Void.class, String.class)
				.field("followers").accessors().build();
		mapper.addToSet(setProperties, setPropertyMeta, "George");

		assertThat(setProperties).hasSize(1);
		assertThat(setProperties).containsKey("followers");
		assertThat(setProperties.get("followers")).containsOnly("test1",
				"test2", "George");
	}

	@Test
	public void should_add_to_empty_map() throws Exception {

		Map<String, Map<Object, Object>> mapProperties = new HashMap<String, Map<Object, Object>>();
		PropertyMeta mapPropertyMeta = PropertyMetaTestBuilder
				.of(CompleteBean.class, Integer.class, String.class)
				.field("preferences").type(MAP).mapper(objectMapper)
				.accessors().build();
		mapper.addToMap(mapProperties, mapPropertyMeta, 1, "FR");

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
		mapProperties.put("preferences", map);

		PropertyMeta mapPropertyMeta = PropertyMetaTestBuilder
				.of(CompleteBean.class, Integer.class, String.class)
				.field("preferences").type(MAP).mapper(objectMapper)
				.accessors().build();

		mapper.addToMap(mapProperties, mapPropertyMeta, 1, "FR");

		assertThat(mapProperties).hasSize(1);
		assertThat(mapProperties).containsKey("preferences");
		assertThat(mapProperties.get("preferences").get(1)).isEqualTo("FR");
		assertThat(mapProperties.get("preferences").get(2)).isEqualTo("Paris");
		assertThat(mapProperties.get("preferences").get(3)).isEqualTo("75014");
	}
}
