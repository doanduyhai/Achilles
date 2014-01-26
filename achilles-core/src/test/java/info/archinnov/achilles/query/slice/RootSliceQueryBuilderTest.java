/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.SliceQueryExecutor;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class RootSliceQueryBuilderTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock(answer = Answers.CALLS_REAL_METHODS)
	private RootSliceQueryBuilder<ClusteredEntity> builder;

	private Class<ClusteredEntity> entityClass = ClusteredEntity.class;

	@Mock
	private SliceQueryExecutor sliceQueryExecutor;

	@Mock
	private ConfigurationContext configContext;

	@Mock
	private EntityMeta meta;

	@Mock
	private PropertyMeta idMeta;

	@Mock
	private List<ClusteredEntity> result;

	@Mock
	private Iterator<ClusteredEntity> iterator;

	@Before
	public void setUp() {
		Whitebox.setInternalState(builder, "sliceQueryExecutor", sliceQueryExecutor);
		Whitebox.setInternalState(builder, "entityClass", (Object) entityClass);
		Whitebox.setInternalState(builder, "meta", meta);
		Whitebox.setInternalState(builder, "idMeta", idMeta);
		Whitebox.setInternalState(builder, "partitionComponents", new ArrayList<Object>());
		Whitebox.setInternalState(builder, "fromClusterings", new ArrayList<Object>());
		Whitebox.setInternalState(builder, "toClusterings", new ArrayList<Object>());

		when(meta.getIdMeta()).thenReturn(idMeta);
		when(meta.getClassName()).thenReturn("entityClass");
		doCallRealMethod().when(builder).partitionComponentsInternal(any());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_set_partition_components() throws Exception {
		builder.partitionComponentsInternal(11L);

		verify(idMeta).validatePartitionComponents(Arrays.<Object> asList(11L));

		assertThat((List<Object>) Whitebox.getInternalState(builder, "partitionComponents")).containsExactly(11L);
	}

	@Test
	public void should_exception_when_partition_components_do_not_match() throws Exception {
		Whitebox.setInternalState(builder, "partitionComponents", Arrays.<Object> asList(10L, "type"));

		exception.expect(AchillesException.class);
		exception.expectMessage("Partition components '[11, type]' do not match previously set values '[10, type]'");
		builder.partitionComponentsInternal(11L, "type");
	}

	@Test
	public void should_exception_when_partition_components_not_same_size_as_previously() throws Exception {
		Whitebox.setInternalState(builder, "partitionComponents", Arrays.<Object> asList(10L, "type"));

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Partition components '[11, type, 11]' do not match previously set values '[10, type]'");
		builder.partitionComponentsInternal(11L, "type", 11);
	}

	@Test
	public void should_set_clustering_from() throws Exception {

		when(idMeta.encodeToComponents(anyListOf(Object.class))).thenReturn(Arrays.<Object> asList(10L, 11L, "a", 12));
		builder.partitionComponentsInternal(10L).fromClusteringsInternal(11L, "a", 12);

		verify(idMeta).validateClusteringComponents(Arrays.<Object> asList(11L, "a", 12));
		assertThat(builder.buildClusterQuery().getClusteringsFrom()).containsExactly(10L, 11L, "a", 12);

	}

	@Test
	public void should_set_clustering_to() throws Exception {
		when(idMeta.encodeToComponents(anyListOf(Object.class))).thenReturn(Arrays.<Object> asList(10L, 11L, "a", 12));
		builder.partitionComponentsInternal(10L).toClusteringsInternal(11L, "a", 12);

		verify(idMeta).validateClusteringComponents(Arrays.<Object> asList(11L, "a", 12));

		assertThat(builder.buildClusterQuery().getClusteringsTo()).containsExactly(10L, 11L, "a", 12);
	}

	@Test
	public void should_set_ordering() throws Exception {
		builder.partitionComponentsInternal(10L).ordering(DESCENDING);

		assertThat(builder.buildClusterQuery().getOrdering()).isEqualTo(DESCENDING);
	}

	@Test
	public void should_exception_when_null_ordering() throws Exception {
		exception.expect(AchillesException.class);
		exception.expectMessage("Ordering mode for slice query for entity 'entityClass' should not be null");

		builder.partitionComponentsInternal(10L).ordering(null);
	}

	@Test
	public void should_set_bounding_mode() throws Exception {
		builder.partitionComponentsInternal(10L).bounding(EXCLUSIVE_BOUNDS);

		assertThat(builder.buildClusterQuery().getBounding()).isEqualTo(EXCLUSIVE_BOUNDS);
	}

	@Test
	public void should_exception_when_null_bounding() throws Exception {
		exception.expect(AchillesException.class);
		exception.expectMessage("Bounding mode for slice query for entity 'entityClass' should not be null");

		builder.partitionComponentsInternal(10L).bounding(null);
	}

	@Test
	public void should_set_consistency_level() throws Exception {
		builder.partitionComponentsInternal(10L).consistencyLevelInternal(EACH_QUORUM);

		assertThat(builder.buildClusterQuery().getConsistencyLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_null_consistency_level() throws Exception {
		exception.expect(AchillesException.class);
		exception.expectMessage("ConsistencyLevel for slice query for entity 'entityClass' should not be null");

		builder.partitionComponentsInternal(10L).consistencyLevelInternal(null);
	}

	@Test
	public void should_set_limit() throws Exception {
		builder.partitionComponentsInternal(10L).limit(53);

		assertThat(builder.buildClusterQuery().getLimit()).isEqualTo(53);
	}

	@Test
	public void should_get() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(result);

		List<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).get();

		assertThat(actual).isSameAs(result);
	}

	@Test
	public void should_get_n() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(result);

		List<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).get(5);

		assertThat(actual).isSameAs(result);
		assertThat(Whitebox.<Integer> getInternalState(builder, "limit")).isEqualTo(5);
	}

	@Test
	public void should_get_first() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		ClusteredEntity entity = new ClusteredEntity();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(Arrays.asList(entity));

		ClusteredEntity actual = builder.partitionComponentsInternal(partitionKey).getFirstOccurence();

		assertThat(actual).isSameAs(entity);

		assertThat(Whitebox.<Integer> getInternalState(builder, "limit")).isEqualTo(1);
	}

	@Test
	public void should_get_first_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		ClusteredEntity entity = new ClusteredEntity();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(Arrays.asList(entity));

		Object[] clusteringComponents = new Object[] { 1, "name" };
		ClusteredEntity actual = builder.partitionComponentsInternal(partitionKey).getFirstOccurence(
				clusteringComponents);

		assertThat(actual).isSameAs(entity);

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
	}

	@Test
	public void should_return_null_when_no_first() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(new ArrayList<ClusteredEntity>());

		ClusteredEntity actual = builder.partitionComponentsInternal(partitionKey).getFirstOccurence();

		assertThat(actual).isNull();

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
	}

	@Test
	public void should_get_first_n() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(result);

		builder.partitionComponentsInternal(partitionKey).getFirst(3);

		assertThat(Whitebox.<Integer> getInternalState(builder, "limit")).isEqualTo(3);
	}

	@Test
	public void should_get_first_n_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(result);

		Object[] clusteringComponents = new Object[] { 1, "name" };
		builder.partitionComponentsInternal(partitionKey).getFirst(3, clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(3);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
	}

	@Test
	public void should_get_last() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		ClusteredEntity entity = new ClusteredEntity();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(Arrays.asList(entity));

		ClusteredEntity actual = builder.partitionComponentsInternal(partitionKey).getLastOccurence();

		assertThat(actual).isSameAs(entity);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
	}

	@Test
	public void should_get_last_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		ClusteredEntity entity = new ClusteredEntity();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(Arrays.asList(entity));

		Object[] clusteringComponents = new Object[] { 1, "name" };

		ClusteredEntity actual = builder.partitionComponentsInternal(partitionKey).getLastOccurence(
				clusteringComponents);

		assertThat(actual).isSameAs(entity);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
	}

	@Test
	public void should_get_last_n() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(result);

		builder.partitionComponentsInternal(partitionKey).getLast(6);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(6);
	}

	@Test
	public void should_get_last_n_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		when(sliceQueryExecutor.get(anySliceQuery())).thenReturn(result);

		Object[] clusteringComponents = new Object[] { 1, "name" };
		builder.partitionComponentsInternal(partitionKey).getLast(6, clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(6);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);

	}

	@Test
	public void should_get_iterator() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		when(sliceQueryExecutor.iterator(anySliceQuery())).thenReturn(iterator);
		Iterator<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).iterator();

		assertThat(actual).isSameAs(iterator);
	}

	@Test
	public void should_get_iterator_with_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		Object[] clusteringComponents = new Object[] { 1, "name" };

		when(sliceQueryExecutor.iterator(anySliceQuery())).thenReturn(iterator);
		Iterator<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).iteratorWithComponents(
				clusteringComponents);

		assertThat(actual).isSameAs(iterator);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);

	}

	@Test
	public void should_get_iterator_with_batch_size() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		when(sliceQueryExecutor.iterator(anySliceQuery())).thenReturn(iterator);
		Iterator<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).iterator(7);

		assertThat(Whitebox.getInternalState(builder, "batchSize")).isEqualTo(7);
		assertThat(actual).isSameAs(iterator);
	}

	@Test
	public void should_get_iterator_with_batch_size_and_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		Object[] clusteringComponents = new Object[] { 1, "name" };

		when(sliceQueryExecutor.iterator(anySliceQuery())).thenReturn(iterator);
		Iterator<ClusteredEntity> actual = builder.partitionComponentsInternal(partitionKey).iteratorWithComponents(7,
				clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "batchSize")).isEqualTo(7);
		assertThat(actual).isSameAs(iterator);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);

	}

	@Test
	public void should_remove() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		builder.partitionComponentsInternal(partitionKey).remove();

		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_n() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		builder.partitionComponentsInternal(partitionKey).remove(8);

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(8);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_first() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		builder.partitionComponentsInternal(partitionKey).removeFirstOccurence();

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_first_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();

		Object[] clusteringComponents = new Object[] { 1, "name" };

		builder.partitionComponentsInternal(partitionKey).removeFirstOccurence(clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_first_n() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		builder.partitionComponentsInternal(partitionKey).removeFirst(9);

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(9);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_first_n_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		Object[] clusteringComponents = new Object[] { 1, "name" };
		builder.partitionComponentsInternal(partitionKey).removeFirst(9, clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(9);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_last() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		builder.partitionComponentsInternal(partitionKey).removeLastOccurence();

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);

		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_last_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		Object[] clusteringComponents = new Object[] { 1, "name" };
		builder.partitionComponentsInternal(partitionKey).removeLastOccurence(clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(1);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_last_n() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		builder.partitionComponentsInternal(partitionKey).removeLast(10);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(10);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	@Test
	public void should_remove_last_n_with_clustering_components() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		Object[] clusteringComponents = new Object[] { 1, "name" };
		builder.partitionComponentsInternal(partitionKey).removeLast(10, clusteringComponents);

		assertThat(Whitebox.getInternalState(builder, "ordering")).isEqualTo(DESCENDING);
		assertThat(Whitebox.getInternalState(builder, "limit")).isEqualTo(10);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "fromClusterings")).containsExactly(
				clusteringComponents);
		assertThat(Whitebox.<List<Object>> getInternalState(builder, "toClusterings")).containsExactly(
				clusteringComponents);
		verify(sliceQueryExecutor).remove(anySliceQuery());
	}

	private SliceQuery<ClusteredEntity> anySliceQuery() {
		return Mockito.<SliceQuery<ClusteredEntity>> any();
	}
}
