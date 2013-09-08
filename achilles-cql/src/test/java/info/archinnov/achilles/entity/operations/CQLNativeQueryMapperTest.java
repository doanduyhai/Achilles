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
package info.archinnov.achilles.entity.operations;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
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

@RunWith(MockitoJUnitRunner.class)
public class CQLNativeQueryMapperTest {

	@InjectMocks
	private CQLNativeQueryMapper mapper;

	@Mock
	private CQLRowMethodInvoker cqlRowInvoker;

	@Mock
	private Row row;

	@Mock
	private ColumnDefinitions columnDefs;

	private Definition def1;

	private Definition def2;

	@Test
	public void should_map_rows() throws Exception {
		Long id = RandomUtils.nextLong();
		String name = "name";

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

		when(cqlRowInvoker.invokeOnRowForType(row, Long.class, "id"))
				.thenReturn(id);
		when(cqlRowInvoker.invokeOnRowForType(row, String.class, "name"))
				.thenReturn(name);

		List<Map<String, Object>> result = mapper.mapRows(Arrays.asList(row));

		verify(cqlRowInvoker).invokeOnRowForType(row, Long.class, "id");
		verify(cqlRowInvoker).invokeOnRowForType(row, String.class, "name");

		assertThat(result).hasSize(1);
		Map<String, Object> line = result.get(0);

		assertThat(line).hasSize(2);
		assertThat(line.get("id")).isEqualTo(id);
		assertThat(line.get("name")).isEqualTo(name);

	}

	@Test
	public void should_map_rows_with_list() throws Exception {
		ArrayList<String> friends = new ArrayList<String>();

		ColumnIdentifier iden1 = new ColumnIdentifier(
				UTF8Type.instance.decompose("friends"), UTF8Type.instance);
		ColumnSpecification spec1 = new ColumnSpecification("keyspace",
				"friends", iden1, ListType.getInstance(UTF8Type.instance));

		def1 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec1);

		when(row.getColumnDefinitions()).thenReturn(columnDefs);
		when(columnDefs.iterator()).thenReturn(Arrays.asList(def1).iterator());

		when(row.getList("friends", String.class)).thenReturn(friends);
		List<Map<String, Object>> result = mapper.mapRows(Arrays.asList(row));

		assertThat(result).hasSize(1);
		Map<String, Object> line = result.get(0);

		assertThat(line).hasSize(1);
		assertThat(line.get("friends")).isSameAs(friends);
	}

	@Test
	public void should_map_rows_with_set() throws Exception {
		Set<String> followers = new HashSet<String>();

		ColumnIdentifier iden1 = new ColumnIdentifier(
				UTF8Type.instance.decompose("followers"), UTF8Type.instance);
		ColumnSpecification spec1 = new ColumnSpecification("keyspace",
				"followers", iden1, SetType.getInstance(UTF8Type.instance));

		def1 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec1);

		when(row.getColumnDefinitions()).thenReturn(columnDefs);
		when(columnDefs.iterator()).thenReturn(Arrays.asList(def1).iterator());

		when(row.getSet("followers", String.class)).thenReturn(followers);
		List<Map<String, Object>> result = mapper.mapRows(Arrays.asList(row));

		assertThat(result).hasSize(1);
		Map<String, Object> line = result.get(0);

		assertThat(line).hasSize(1);
		assertThat(line.get("followers")).isSameAs(followers);
	}

	@Test
	public void should_map_rows_with_map() throws Exception {
		Map<BigInteger, String> preferences = new HashMap<BigInteger, String>();

		ColumnIdentifier iden1 = new ColumnIdentifier(
				UTF8Type.instance.decompose("preferences"), UTF8Type.instance);
		ColumnSpecification spec1 = new ColumnSpecification("keyspace",
				"followers", iden1, MapType.getInstance(IntegerType.instance,
						UTF8Type.instance));

		def1 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec1);

		when(row.getColumnDefinitions()).thenReturn(columnDefs);
		when(columnDefs.iterator()).thenReturn(Arrays.asList(def1).iterator());

		when(row.getMap("preferences", BigInteger.class, String.class))
				.thenReturn(preferences);
		List<Map<String, Object>> result = mapper.mapRows(Arrays.asList(row));

		assertThat(result).hasSize(1);
		Map<String, Object> line = result.get(0);

		assertThat(line).hasSize(1);
		assertThat(line.get("preferences")).isSameAs(preferences);
	}

	@Test
	public void should_return_empty_list_when_no_column() throws Exception {
		ColumnIdentifier iden1 = new ColumnIdentifier(
				UTF8Type.instance.decompose("id"), UTF8Type.instance);
		ColumnSpecification spec1 = new ColumnSpecification("keyspace", "id",
				iden1, LongType.instance);

		def1 = Whitebox.invokeMethod(Definition.class,
				"fromTransportSpecification", spec1);

		when(row.getColumnDefinitions()).thenReturn(null);

		List<Map<String, Object>> result = mapper.mapRows(Arrays.asList(row));
		assertThat(result).isEmpty();

		verifyZeroInteractions(cqlRowInvoker);
	}

	@Test
	public void should_return_empty_list_when_no_row() throws Exception {
		List<Map<String, Object>> result = mapper.mapRows(new ArrayList<Row>());
		assertThat(result).isEmpty();

		verifyZeroInteractions(cqlRowInvoker);
	}
}
