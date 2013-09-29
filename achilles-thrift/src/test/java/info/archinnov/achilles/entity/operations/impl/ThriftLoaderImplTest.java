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
package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.HCounterColumnImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftLoaderImplTest {

	@InjectMocks
	private ThriftLoaderImpl loaderImpl;

	@Mock
	private ThriftEntityMapper mapper;

	@Mock
	private ThriftCompositeFactory compositeFactory;

	@Mock
	private ThriftCompositeTransformer compositeTransformer;

	@Mock
	private PropertyMeta idMeta;

	@Mock
	private PropertyMeta pm;

	@Mock
	private EntityMeta entityMeta;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private ThriftPersistenceContext context;

	@Mock
	private Object primaryKey;

	@Mock
	private Object rowKey;

	@Mock
	private CompleteBean entity;

	@Before
	public void setUp() {
		when(compositeFactory.buildRowKey(context)).thenReturn(rowKey);
		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when((Class<CompleteBean>) context.getEntityClass()).thenReturn(CompleteBean.class);
	}

	@Test
	public void should_load() throws Exception {

		Composite comp = new Composite();
		List<Pair<Composite, String>> values = new ArrayList<Pair<Composite, String>>();
		values.add(Pair.create(comp, "value"));

		when(context.getEntityDao().eagerFetchEntity(rowKey)).thenReturn(values);
		when(entityMeta.instanciate()).thenReturn(entity);

		CompleteBean actual = loaderImpl.load(context, CompleteBean.class);

		assertThat(actual).isSameAs(entity);
		verify(mapper).setEagerPropertiesToEntity(primaryKey, values, entityMeta, entity);
	}

	@Test
	public void should_load_clustered_entity() throws Exception {
		Composite comp = new Composite();
		Object clusteredValue = "clusteredValue";
		BeanWithClusteredId expected = new BeanWithClusteredId();
		HColumn<Composite, Object> hCol = new HColumnImpl<Composite, Object>(comp, clusteredValue, 10);

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getColumn(rowKey, comp)).thenReturn(hCol);
		when(compositeTransformer.buildClusteredEntity(BeanWithClusteredId.class, context, hCol)).thenReturn(expected);

		BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

		assertThat(actual).isSameAs(expected);
	}

	@Test
	public void should_return_null_when_no_clustered_entity() throws Exception {
		Composite comp = new Composite();

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getColumn(rowKey, comp)).thenReturn(null);

		assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

		verifyZeroInteractions(compositeTransformer);
	}

	@Test
	public void should_load_counter_clustered_entity() throws Exception {

		Composite comp = new Composite();
		BeanWithClusteredId expected = new BeanWithClusteredId();
		HCounterColumn<Composite> hCounterCol = new HCounterColumnImpl<Composite>(comp, 10L);

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getCounterColumn(rowKey, comp)).thenReturn(hCounterCol);
		when(
				compositeTransformer.buildClusteredEntityWithIdOnly(BeanWithClusteredId.class, context, hCounterCol
						.getName().getComponents())).thenReturn(expected);

		BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

		assertThat(actual).isSameAs(expected);
	}

	@Test
	public void should_return_null_when_no_counter_clustered_entity() throws Exception {
		Composite comp = new Composite();

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		when(entityMeta.isClusteredCounter()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getCounterColumn(rowKey, comp)).thenReturn(null);

		assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

		verifyZeroInteractions(compositeTransformer);
	}

	@Test
	public void should_load_value_less_clustered_entity() throws Exception {
		Composite comp = new Composite();
		BeanWithClusteredId expected = new BeanWithClusteredId();
		HColumn<Composite, Object> hCol = new HColumnImpl<Composite, Object>(comp, "", 10);

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		when(entityMeta.isValueless()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getColumn(rowKey, comp)).thenReturn(hCol);
		when(
				compositeTransformer.buildClusteredEntityWithIdOnly(BeanWithClusteredId.class, context, hCol.getName()
						.getComponents())).thenReturn(expected);

		BeanWithClusteredId actual = loaderImpl.load(context, BeanWithClusteredId.class);

		assertThat(actual).isSameAs(expected);
	}

	@Test
	public void should_return_null_when_no_value_less_clustered_entity() throws Exception {
		Composite comp = new Composite();

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		when(entityMeta.isValueless()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getColumn(rowKey, comp)).thenReturn(null);

		assertThat(loaderImpl.load(context, BeanWithClusteredId.class)).isNull();

		verifyZeroInteractions(compositeTransformer);
	}

	@Test
	public void should_load_simple_property_for_entity() throws Exception {

		String value = "value";
		Composite comp = new Composite();
		when(compositeFactory.createBaseForGet(pm)).thenReturn(comp);
		when(context.getEntityDao().getValue(rowKey, comp)).thenReturn(value);
		when(pm.forceDecodeFromJSON(value)).thenReturn(value);

		Object actual = loaderImpl.loadSimpleProperty(context, pm);
		assertThat(actual).isEqualTo(value);
	}

	@Test
	public void should_load_simple_property_for_clustered_entity() throws Exception {

		String value = "value";
		Composite comp = new Composite();
		when(context.isClusteredEntity()).thenReturn(true);
		when(compositeFactory.createBaseForClusteredGet(primaryKey, idMeta)).thenReturn(comp);
		when(context.getWideRowDao().getValue(rowKey, comp)).thenReturn(value);
		when(pm.decode(value)).thenReturn(value);

		Object actual = loaderImpl.loadSimpleProperty(context, pm);
		verify(compositeFactory).createBaseForClusteredGet(primaryKey, idMeta);
		assertThat(actual).isEqualTo(value);
	}

	@Test
	public void should_load_list() throws Exception {

		Composite start = new Composite(), end = new Composite();
		start.addComponent(LIST.flag(), BYTE_SRZ);
		start.addComponent("friends", STRING_SRZ);
		start.addComponent("0", STRING_SRZ);

		end.addComponent(LIST.flag(), BYTE_SRZ);
		end.addComponent("friends", STRING_SRZ);
		end.addComponent("1", STRING_SRZ);

		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
		columns.add(Pair.<Composite, Object> create(start, "foo"));
		columns.add(Pair.<Composite, Object> create(end, "bar"));

		when(compositeFactory.createBaseForQuery(pm, EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(pm, GREATER_THAN_EQUAL)).thenReturn(end);
		when(context.getEntityDao().findColumnsRange(rowKey, start, end, false, Integer.MAX_VALUE)).thenReturn(columns);
		when(pm.decode("foo")).thenReturn("foo");
		when(pm.decode("bar")).thenReturn("bar");

		List<Object> actual = loaderImpl.loadListProperty(context, pm);
		assertThat(actual).containsExactly("foo", "bar");
	}

	@Test
	public void should_load_set() throws Exception {

		Composite start = new Composite(), end = new Composite();
		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();
		columns.add(Pair.<Composite, Object> create(start, "John"));
		columns.add(Pair.<Composite, Object> create(end, "Helen"));

		start.addComponent(SET.flag(), BYTE_SRZ);
		start.addComponent("friends", STRING_SRZ);
		start.addComponent("John", STRING_SRZ);

		end.addComponent(SET.flag(), BYTE_SRZ);
		end.addComponent("friends", STRING_SRZ);
		end.addComponent("Helen", STRING_SRZ);

		when(compositeFactory.createBaseForQuery(pm, EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(pm, GREATER_THAN_EQUAL)).thenReturn(end);
		when(context.getEntityDao().findColumnsRange(rowKey, start, end, false, Integer.MAX_VALUE)).thenReturn(columns);
		when(pm.decode("John")).thenReturn("John");
		when(pm.decode("Helen")).thenReturn("Helen");

		Set<Object> actual = loaderImpl.loadSetProperty(context, pm);
		assertThat(actual).containsExactly("John", "Helen");
	}

	@Test
	public void should_load_map() throws Exception {

		Composite start = new Composite(), end = new Composite();
		start.addComponent(MAP.flag(), BYTE_SRZ);
		start.addComponent("friends", STRING_SRZ);
		start.addComponent("1", STRING_SRZ);

		end.addComponent(MAP.flag(), BYTE_SRZ);
		end.addComponent("friends", STRING_SRZ);
		end.addComponent("2", STRING_SRZ);

		List<Pair<Composite, Object>> columns = new ArrayList<Pair<Composite, Object>>();

		UserBean user1 = new UserBean(), user2 = new UserBean();
		user1.setName("user1");
		user2.setName("user2");

		columns.add(Pair.<Composite, Object> create(start, "user1"));
		columns.add(Pair.<Composite, Object> create(end, "user2"));

		when(compositeFactory.createBaseForQuery(pm, EQUAL)).thenReturn(start);
		when(compositeFactory.createBaseForQuery(pm, GREATER_THAN_EQUAL)).thenReturn(end);
		when(context.getEntityDao().findColumnsRange(rowKey, start, end, false, Integer.MAX_VALUE)).thenReturn(columns);
		when((Class) pm.getKeyClass()).thenReturn(Integer.class);
		when(pm.forceDecodeFromJSON("1", Integer.class)).thenReturn(1);
		when(pm.forceDecodeFromJSON("2", Integer.class)).thenReturn(2);
		when(pm.decode("user1")).thenReturn(user1);
		when(pm.decode("user2")).thenReturn(user2);

		Map<Object, Object> actual = loaderImpl.loadMapProperty(context, pm);
		assertThat(actual).hasSize(2);
		assertThat(((UserBean) actual.get(1)).getName()).isEqualTo("user1");
		assertThat(((UserBean) actual.get(2)).getName()).isEqualTo("user2");
	}
}
