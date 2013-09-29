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
package info.archinnov.achilles.entity;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.EntityMetaTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;

import org.apache.cassandra.utils.Pair;
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

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityMapperTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftEntityMapper mapper;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private ThriftPersistenceContext context;

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

	@Before
	public void setUp() throws Exception {
		idMeta = PropertyMetaTestBuilder.of(CompleteBean.class, Void.class, Long.class).field("id").invoker(invoker)
				.build();
	}

	@Test
	public void should_map_columns_to_bean() throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta namePropertyMeta = PropertyMetaTestBuilder.of(CompleteBean.class, Void.class, String.class)
				.field("name").type(SIMPLE).mapper(objectMapper).accessors().invoker(invoker).build();

		PropertyMeta listPropertyMeta = PropertyMetaTestBuilder.of(CompleteBean.class, Void.class, String.class)
				.field("friends").type(LIST).mapper(objectMapper).accessors().invoker(invoker).build();

		PropertyMeta setPropertyMeta = PropertyMetaTestBuilder.of(CompleteBean.class, Void.class, String.class)
				.field("followers").type(SET).mapper(objectMapper).accessors().invoker(invoker).build();

		PropertyMeta mapPropertyMeta = PropertyMetaTestBuilder.of(CompleteBean.class, Integer.class, String.class)
				.field("preferences").type(MAP).mapper(objectMapper).invoker(invoker).accessors().build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta).addPropertyMeta(namePropertyMeta)
				.addPropertyMeta(listPropertyMeta).addPropertyMeta(setPropertyMeta).addPropertyMeta(mapPropertyMeta)
				.build();

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(Pair.create(buildSimplePropertyComposite("name"), "name"));

		columns.add(Pair.create(buildListPropertyComposite("friends", 0), "foo"));
		columns.add(Pair.create(buildListPropertyComposite("friends", 1), "bar"));

		columns.add(Pair.create(buildSetPropertyComposite("followers", "George"), ""));
		columns.add(Pair.create(buildSetPropertyComposite("followers", "Paul"), ""));

		columns.add(Pair.create(buildMapPropertyComposite("preferences", 1), "FR"));
		columns.add(Pair.create(buildMapPropertyComposite("preferences", 2), "Paris"));
		columns.add(Pair.create(buildMapPropertyComposite("preferences", 3), "75014"));

		doNothing().when(invoker).setValueToField(eq(entity), eq(idMeta.getSetter()), idCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()), simpleCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(setPropertyMeta.getSetter()), setCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(listPropertyMeta.getSetter()), listCaptor.capture());
		doNothing().when(invoker).setValueToField(eq(entity), eq(mapPropertyMeta.getSetter()), mapCaptor.capture());

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		assertThat(idCaptor.getValue()).isEqualTo(2L);
		assertThat(simpleCaptor.getValue()).isEqualTo("name");

		assertThat(listCaptor.getValue()).hasSize(2);
		assertThat(listCaptor.getValue()).contains("foo", "bar");

		assertThat(setCaptor.getValue()).hasSize(2);
		assertThat(setCaptor.getValue()).contains("George", "Paul");

		assertThat(mapCaptor.getValue()).hasSize(3);
		assertThat(mapCaptor.getValue().get(1)).isEqualTo("FR");
		assertThat(mapCaptor.getValue().get(2)).isEqualTo("Paris");
		assertThat(mapCaptor.getValue().get(3)).isEqualTo("75014");
	}

	@Test
	public void should_do_nothing_for_unmapped_property() throws Exception {
		CompleteBean entity = new CompleteBean();

		PropertyMeta namePropertyMeta = PropertyMetaTestBuilder.of(CompleteBean.class, Void.class, String.class)
				.field("name").type(SIMPLE).mapper(objectMapper).accessors().invoker(invoker).build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.addPropertyMeta(namePropertyMeta).build();

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(Pair.create(buildSimplePropertyComposite("name"), "name"));
		columns.add(Pair.create(buildSimplePropertyComposite("unmapped"), "unmapped property"));

		doNothing().when(invoker).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()), simpleCaptor.capture());

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		verify(invoker).setValueToField(eq(entity), eq(namePropertyMeta.getSetter()), any(List.class));

		assertThat(simpleCaptor.getValue()).isEqualTo("name");

	}

	@Test
	public void should_not_set_lazy_or_proxy_property_type() throws Exception {
		CompleteBean entity = new CompleteBean();

		PropertyMeta lazyNamePropertyMeta = PropertyMetaTestBuilder
				//
				.of(CompleteBean.class, Void.class, String.class).field("name").type(LAZY_SIMPLE).mapper(objectMapper)
				.accessors().build();

		entityMeta = EntityMetaTestBuilder.builder(idMeta) //
				.addPropertyMeta(lazyNamePropertyMeta).build();

		List<Pair<Composite, String>> columns = new ArrayList<Pair<Composite, String>>();

		columns.add(Pair.create(buildSimplePropertyComposite("name"), "name"));

		mapper.setEagerPropertiesToEntity(2L, columns, entityMeta, entity);

		verify(invoker, never()).setValueToField(entity, lazyNamePropertyMeta.getSetter(), "name");

	}

	@Test
	public void should_init_clustered_entity() throws Exception {
		BeanWithClusteredId entity = new BeanWithClusteredId();
		EmbeddedKey embeddedKey = new EmbeddedKey();
		entityMeta = mock(EntityMeta.class);

		when(entityMeta.instanciate()).thenReturn(entity);

		BeanWithClusteredId actual = mapper.initClusteredEntity(BeanWithClusteredId.class, entityMeta, embeddedKey);

		assertThat(actual).isSameAs(entity);

		verify(entityMeta).setPrimaryKey(entity, embeddedKey);
	}

	@Test
	public void should_create_clustered_entity_with_value() throws Exception {
		BeanWithClusteredId entity = new BeanWithClusteredId();
		EmbeddedKey embeddedKey = new EmbeddedKey();
		String clusteredValue = "clusteredValue";

		PropertyMeta pm = mock(PropertyMeta.class);
		entityMeta = mock(EntityMeta.class);

		when(context.getIdMeta()).thenReturn(idMeta);
		when(context.getFirstMeta()).thenReturn(pm);

		when(entityMeta.instanciate()).thenReturn(entity);

		BeanWithClusteredId actual = mapper.createClusteredEntityWithValue(BeanWithClusteredId.class, entityMeta, pm,
				embeddedKey, clusteredValue);

		assertThat(actual).isSameAs(entity);

		verify(entityMeta).setPrimaryKey(entity, embeddedKey);
		verify(pm).setValueToField(entity, clusteredValue);
	}

	private Composite buildSimplePropertyComposite(String propertyName) {
		Composite comp = new Composite();
		comp.add(0, SIMPLE.flag());
		comp.add(1, propertyName);
		comp.add(2, "0");
		return comp;
	}

	private Composite buildListPropertyComposite(String propertyName, int index) {
		Composite comp = new Composite();
		comp.add(0, LIST.flag());
		comp.add(1, propertyName);
		comp.add(2, index + "");
		return comp;
	}

	private Composite buildSetPropertyComposite(String propertyName, String value) {
		Composite comp = new Composite();
		comp.add(0, SET.flag());
		comp.add(1, propertyName);
		comp.add(2, value);
		return comp;
	}

	private Composite buildMapPropertyComposite(String propertyName, int key) {
		Composite comp = new Composite();
		comp.add(0, MAP.flag());
		comp.add(1, propertyName);
		comp.add(2, key + "");
		return comp;
	}
}
