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
package info.archinnov.achilles.iterator;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.HColumnTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftSliceIteratorTest {

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private SliceQuery<Long, Composite, String> query;

	@Mock
	private QueryResult<ColumnSlice<Composite, String>> queryResult;

	@Mock
	private ColumnSlice<Composite, String> columnSlice;

	@Mock
	private List<HColumn<Composite, String>> hColumns;

	@Mock
	private Iterator<HColumn<Composite, String>> columnsIterator;

	private ThriftSliceIterator<Long, String> iterator;

	@Mock
	private ThriftConsistencyLevelPolicy policy;

	private String columnFamily = "cf";

	@Before
	public void setUp() {
		when(query.execute()).thenReturn(queryResult);
		when(queryResult.get()).thenReturn(columnSlice);
		when(columnSlice.getColumns()).thenReturn(hColumns);
		when(hColumns.iterator()).thenReturn(columnsIterator);
		when((Class) propertyMeta.getValueClass()).thenReturn(UserBean.class);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_3_values() throws Exception {
		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = new Composite(), //
		name2 = new Composite(), //
		name3 = new Composite();

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		String val1 = "val1", val2 = "val2", val3 = "val3";
		int ttl = 10;

		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(name1,
				val1, ttl);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(name2,
				val2, ttl);
		HColumn<Composite, String> hCol3 = HColumnTestBuilder.simple(name3,
				val3, ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, true,
				true, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		when(policy.getCurrentReadLevel()).thenReturn(LOCAL_QUORUM, ONE);

		iterator = new ThriftSliceIterator<Long, String>(policy, columnFamily,
				query, start, end, false, 10);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h1 = iterator.next();

		assertThat(h1.getNameBytes()).isEqualTo(name1.serialize());
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h2 = iterator.next();

		assertThat(h2.getNameBytes()).isEqualTo(name2.serialize());
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h3 = iterator.next();

		assertThat(h3.getNameBytes()).isEqualTo(name3.serialize());
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy, atLeastOnce()).setCurrentReadLevel(LOCAL_QUORUM);
		verify(policy, atLeastOnce()).setCurrentReadLevel(ONE);
		verify(policy).loadConsistencyLevelForRead(columnFamily);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_reload_when_reaching_end_of_batch() throws Exception {
		Composite start = new Composite(), //
		end = new Composite(), //
		name1 = new Composite(), //
		name2 = new Composite(), //
		name3 = new Composite();
		int count = 2;

		name1.addComponent("name1", STRING_SRZ);
		name2.addComponent("name2", STRING_SRZ);
		name3.addComponent("name3", STRING_SRZ);

		String val1 = "val1", val2 = "val2", val3 = "val3";
		int ttl = 10;

		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(name1,
				val1, ttl);
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(name2,
				val2, ttl);
		HColumn<Composite, String> hCol3 = HColumnTestBuilder.simple(name3,
				val3, ttl);

		when(columnsIterator.hasNext()).thenReturn(true, true, true, false,
				true, false, false);
		when(columnsIterator.next()).thenReturn(hCol1, hCol2, hCol3);

		iterator = new ThriftSliceIterator<Long, String>(policy, columnFamily,
				query, start, end, false, count);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h1 = iterator.next();

		assertThat(h1.getNameBytes()).isEqualTo(name1.serialize());
		assertThat(h1.getValue()).isEqualTo(val1);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h2 = iterator.next();

		assertThat(h2.getNameBytes()).isEqualTo(name2.serialize());
		assertThat(h2.getValue()).isEqualTo(val2);

		assertThat(iterator.hasNext()).isEqualTo(true);
		HColumn<Composite, String> h3 = iterator.next();

		assertThat(h3.getNameBytes()).isEqualTo(name3.serialize());
		assertThat(h3.getValue()).isEqualTo(val3);

		assertThat(iterator.hasNext()).isEqualTo(false);

		verify(policy).getCurrentReadLevel();
		verify(policy, never())
				.setCurrentReadLevel(any(ConsistencyLevel.class));

	}
}
