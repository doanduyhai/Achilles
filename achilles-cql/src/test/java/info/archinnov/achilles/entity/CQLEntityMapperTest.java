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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityMapperTest {

	@InjectMocks
	private CQLEntityMapper entityMapper;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLRowMethodInvoker cqlRowInvoker;

	@Mock
	private Row row;

	@Mock
	private ColumnDefinitions columnDefs;

	@Mock
	private EntityMeta entityMeta;

	private Definition def1;
	private Definition def2;

	private CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
			.buid();

	@Test
	public void should_set_eager_properties_to_entity() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id").accessors()
				.type(PropertyType.ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.SIMPLE).build();

		List<PropertyMeta> eagerMetas = Arrays.asList(pm);

		when((PropertyMeta) entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.getEagerMetas()).thenReturn(eagerMetas);

		when(row.isNull("name")).thenReturn(false);
		when(cqlRowInvoker.invokeOnRowForFields(row, pm)).thenReturn("value");

		entityMapper.setEagerPropertiesToEntity(row, entityMeta, entity);

		verify(invoker).setValueToField(entity, pm.getSetter(), "value");
	}

	@Test
	public void should_set_null_to_entity_when_no_value_from_row()
			throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.SIMPLE).build();

		List<PropertyMeta> eagerMetas = Arrays.asList(pm);

		when(entityMeta.getEagerMetas()).thenReturn(eagerMetas);

		when(row.isNull("name")).thenReturn(true);

		entityMapper.setEagerPropertiesToEntity(row, entityMeta, entity);

		verifyZeroInteractions(cqlRowInvoker, invoker);
	}

	@Test
	public void should_do_nothing_when_null_row() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.SIMPLE).build();

		entityMapper.setPropertyToEntity((Row) null, pm, entity);

		verifyZeroInteractions(cqlRowInvoker, invoker);
	}

	@Test
	public void should_set_property_to_entity() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.SIMPLE).build();

		entityMapper.setJoinValueToEntity("name", pm, entity);

		verify(invoker).setValueToField(entity, pm.getSetter(), "name");
	}

	@Test
	public void should_set_compound_key_to_entity() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(PropertyType.EMBEDDED_ID).compNames("name")
				.build();

		CompoundKey compoundKey = new CompoundKey();
		when(cqlRowInvoker.invokeOnRowForCompoundKey(row, pm)).thenReturn(
				compoundKey);

		entityMapper.setPropertyToEntity(row, pm, entity);

		verify(invoker).setValueToField(entity, pm.getSetter(), compoundKey);
	}

	@Test
	public void should_map_row_to_entity() throws Exception {
		Long id = RandomUtils.nextLong();
		String name = "name";
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).accessors().build();

		Map<String, PropertyMeta> propertiesMap = ImmutableMap.of("id", idMeta);

		ColumnIdentifier iden1 = new ColumnIdentifier(
				UTF8Type.instance.decompose("id"), UTF8Type.instance);
		ColumnSpecification spec1 = new ColumnSpecification("keyspace", "id",
				iden1, LongType.instance);

		ColumnIdentifier iden2 = new ColumnIdentifier(
				UTF8Type.instance.decompose(name), UTF8Type.instance);
		ColumnSpecification spec2 = new ColumnSpecification("keyspace", "name",
				iden2, UTF8Type.instance);

		def1 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec1);
		def2 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec2);

		when(row.getColumnDefinitions()).thenReturn(columnDefs);
		when(columnDefs.iterator()).thenReturn(
				Arrays.asList(def1, def2).iterator());

		when(invoker.instanciate(CompleteBean.class)).thenReturn(entity);

		when(cqlRowInvoker.invokeOnRowForFields(row, idMeta)).thenReturn(id);

		CompleteBean actual = entityMapper.mapRowToEntity(CompleteBean.class,
				row, propertiesMap);

		assertThat(actual).isSameAs(entity);
		verify(invoker).setValueToField(entity, idMeta.getSetter(), id);
	}

	@Test
	public void should_skip_mapping_join_column() throws Exception {
		Long id = RandomUtils.nextLong();
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("id")
				.type(PropertyType.ID).accessors().build();

		PropertyMeta userMeta = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.type(PropertyType.JOIN_SIMPLE).build();

		Map<String, PropertyMeta> propertiesMap = ImmutableMap.of("id", idMeta,
				"user", userMeta);

		ColumnIdentifier iden1 = new ColumnIdentifier(
				UTF8Type.instance.decompose("id"), UTF8Type.instance);
		ColumnSpecification spec1 = new ColumnSpecification("keyspace", "id",
				iden1, LongType.instance);

		ColumnIdentifier iden2 = new ColumnIdentifier(
				UTF8Type.instance.decompose("user"), UTF8Type.instance);
		ColumnSpecification spec2 = new ColumnSpecification("keyspace", "user",
				iden2, UTF8Type.instance);

		def1 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec1);
		def2 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec2);

		when(row.getColumnDefinitions()).thenReturn(columnDefs);
		when(columnDefs.iterator()).thenReturn(
				Arrays.asList(def1, def2).iterator());

		when(invoker.instanciate(CompleteBean.class)).thenReturn(entity);

		when(cqlRowInvoker.invokeOnRowForFields(row, idMeta)).thenReturn(id);

		CompleteBean actual = entityMapper.mapRowToEntity(CompleteBean.class,
				row, propertiesMap);

		assertThat(actual).isSameAs(entity);
		verify(invoker).setValueToField(entity, idMeta.getSetter(), id);
		verify(cqlRowInvoker, never()).invokeOnRowForFields(row, userMeta);
	}

	@Test
	public void should_return_null_when_no_column_found() throws Exception {
		when(row.getColumnDefinitions()).thenReturn(null);
		CompleteBean actual = entityMapper.mapRowToEntity(CompleteBean.class,
				row, null);
		assertThat(actual).isNull();
	}

}
