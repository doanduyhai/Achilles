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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;

@RunWith(MockitoJUnitRunner.class)
public class CQLSliceQueryIteratorTest {

	private CQLSliceQueryIterator<ClusteredEntity> sliceIterator;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private CQLEntityMapper mapper;

	@Mock
	private CQLRowMethodInvoker cqlInvoker;

	@Mock
	private CQLEntityProxifier proxifier;

	@Mock
	private CQLSliceQuery<ClusteredEntity> sliceQuery;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private CQLPersistenceContext context;

	@Mock
	private Iterator<Row> iterator;

	@Mock
	private PreparedStatement ps;

	private EntityMeta meta = new EntityMeta();

	private int batchSize = 99;

	@Before
	public void setUp() {
		when(sliceQuery.getEntityClass()).thenReturn(ClusteredEntity.class);
		when(sliceQuery.getMeta()).thenReturn(meta);
		when(sliceQuery.getVaryingComponentName()).thenReturn("name");
		when(sliceQuery.getVaryingComponentClass()).thenReturn(
				(Class) String.class);
		when(sliceQuery.getBatchSize()).thenReturn(batchSize);

		sliceIterator = new CQLSliceQueryIterator<ClusteredEntity>(sliceQuery,
				context, iterator, ps);

		Whitebox.setInternalState(sliceIterator, "mapper", mapper);
		Whitebox.setInternalState(sliceIterator, "cqlInvoker", cqlInvoker);
		Whitebox.setInternalState(sliceIterator, "proxifier", proxifier);
	}

	@Test
	public void should_return_true_for_has_next() throws Exception {
		when(iterator.hasNext()).thenReturn(true);

		assertThat(sliceIterator.hasNext()).isTrue();
	}

	@Test
	public void should_reload_data_when_end_of_batch_size() throws Exception {
		Whitebox.setInternalState(sliceIterator, "count", batchSize);
		when(iterator.hasNext()).thenReturn(false);

		when(context.bindAndExecute(ps, "name").iterator())
				.thenReturn(iterator);

		assertThat(sliceIterator.hasNext()).isFalse();

		verify(context).bindAndExecute(ps, "name");
	}

	@Test
	public void should_return_false_for_has_next_when_no_more_data()
			throws Exception {
		when(iterator.hasNext()).thenReturn(false);
		Whitebox.setInternalState(sliceIterator, "count", batchSize - 1);

		assertThat(sliceIterator.hasNext()).isFalse();
	}

	@Test
	public void should_get_next_clustered_entity() throws Exception {
		ClusteredEntity entity = new ClusteredEntity();
		Row row = mock(Row.class);

		meta.setEntityClass(ClusteredEntity.class);
		Whitebox.setInternalState(meta, ReflectionInvoker.class, invoker);

		when(iterator.next()).thenReturn(row);

		when(cqlInvoker.invokeOnRowForType(row, String.class, "name"))
				.thenReturn("name1");
		when(invoker.instanciate(ClusteredEntity.class)).thenReturn(entity);

		when(context.duplicate(entity)).thenReturn(context);
		when(proxifier.buildProxy(entity, context)).thenReturn(entity);

		ClusteredEntity actual = sliceIterator.next();

		assertThat(actual).isSameAs(entity);
		assertThat((Integer) Whitebox.getInternalState(sliceIterator, "count"))
				.isEqualTo(1);
		verify(mapper).setEagerPropertiesToEntity(row, meta, entity);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_calling_remove() throws Exception {
		sliceIterator.remove();
	}
}
