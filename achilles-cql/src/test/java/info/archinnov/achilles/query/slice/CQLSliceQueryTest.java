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
package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.type.BoundingMode.INCLUSIVE_END_BOUND_ONLY;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.datastax.driver.core.querybuilder.Ordering;

@RunWith(MockitoJUnitRunner.class)
public class CQLSliceQueryTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLSliceQuery<ClusteredEntity> cqlSliceQuery;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private SliceQuery<ClusteredEntity> sliceQuery;

	@Mock
	private SliceQueryValidator validator;

	private List<Object> defaultStart = Arrays.<Object> asList(1, 2);
	private List<Object> defaultEnd = Arrays.<Object> asList(1, 2);

	@Before
	public void setUp() {
		when(sliceQuery.getOrdering()).thenReturn(ASCENDING);
		when(sliceQuery.getClusteringsFrom()).thenReturn(defaultStart);
		when(sliceQuery.getClusteringsTo()).thenReturn(defaultEnd);
	}

	@Test
	public void should_get_fixed_components_when_same_size() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getFixedComponents()).containsExactly(11L, "a");
	}

	@Test
	public void should_get_fixed_components_when_start_same_as_end() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getFixedComponents()).containsExactly(11L, "a", 12.0);
	}

	@Test
	public void should_get_fixed_components_when_start_more_than_end() throws Exception {

		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a"));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getFixedComponents()).containsExactly(11L, "a");
	}

	@Test
	public void should_get_last_components_when_same_size() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getLastStartComponent()).isEqualTo(11.0);
		assertThat(cqlSliceQuery.getLastEndComponent()).isEqualTo(12.0);
	}

	@Test
	public void should_get_last_components_when_start_same_as_end() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getLastStartComponent()).isNull();
		assertThat(cqlSliceQuery.getLastEndComponent()).isNull();
	}

	@Test
	public void should_get_last_components_when_start_more_than_end() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a", 11.0));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a"));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getLastStartComponent()).isEqualTo(11.0);
		assertThat(cqlSliceQuery.getLastEndComponent()).isNull();
	}

	@Test
	public void should_get_last_components_when_end_more_than_start() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a"));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, "a", 12.0));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getLastStartComponent()).isNull();
		assertThat(cqlSliceQuery.getLastEndComponent()).isEqualTo(12.0);
	}

	@Test
	public void should_get_limit() throws Exception {
		when(sliceQuery.getLimit()).thenReturn(99);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getLimit()).isEqualTo(99);
	}

	@Test
	public void should_get_cql_consistency_level() throws Exception {
		when(sliceQuery.getConsistencyLevel()).thenReturn(LOCAL_QUORUM);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getConsistencyLevel()).isEqualTo(
				com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_get_default_cql_consistency_level() throws Exception {
		when(sliceQuery.getConsistencyLevel()).thenReturn(null);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getConsistencyLevel())
				.isEqualTo(com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_get_bounding() throws Exception {
		when(sliceQuery.getBounding()).thenReturn(INCLUSIVE_END_BOUND_ONLY);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getBounding()).isEqualTo(INCLUSIVE_END_BOUND_ONLY);
	}

	@Test
	public void should_get_ordering_asc() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L));
		when(sliceQuery.getMeta().getIdMeta().getOrderingComponent()).thenReturn("orderingComp");
		when(sliceQuery.getOrdering()).thenReturn(ASCENDING);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		Ordering ordering = cqlSliceQuery.getCQLOrdering();
		assertThat(Whitebox.getInternalState(ordering, "name")).isEqualTo("orderingComp");
		assertThat((Boolean) Whitebox.getInternalState(ordering, "isDesc")).isFalse();
	}

	@Test
	public void should_get_ordering_desc() throws Exception {

		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L));
		when(sliceQuery.getMeta().getIdMeta().getOrderingComponent()).thenReturn("orderingComp");
		when(sliceQuery.getOrdering()).thenReturn(DESCENDING);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		Ordering ordering = cqlSliceQuery.getCQLOrdering();
		assertThat(Whitebox.getInternalState(ordering, "name")).isEqualTo("orderingComp");
		assertThat((Boolean) Whitebox.getInternalState(ordering, "isDesc")).isTrue();
	}

	@Test
	public void should_get_components_name() throws Exception {

		when(sliceQuery.getIdMeta().getComponentNames()).thenReturn(Arrays.asList("id", "count", "name"));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getComponentNames()).containsExactly("id", "count", "name");
	}

	@Test
	public void should_get_varying_component_name() throws Exception {

		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, 2, "a"));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, 2));

		when(sliceQuery.getIdMeta().getVaryingComponentNameForQuery(2)).thenReturn("name");

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getVaryingComponentName()).isEqualTo("name");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void should_get_varying_component_class() throws Exception {
		when(sliceQuery.getOrdering()).thenReturn(ASCENDING);
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, 2));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L, 3));

		when(sliceQuery.getIdMeta().getVaryingComponentClassForQuery(1)).thenReturn((Class) Integer.class);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getVaryingComponentClass()).isSameAs((Class) Integer.class);
	}

	@Test
	public void should_get_meta() throws Exception {
		EntityMeta meta = new EntityMeta();
		when(sliceQuery.getMeta()).thenReturn(meta);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getMeta()).isSameAs(meta);
	}

	@Test
	public void should_get_achilles_ordering() throws Exception {
		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getOrdering()).isSameAs(ASCENDING);
	}

	@Test
	public void should_exception_when_varying_components_for_remove() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L, "a"));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L));

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		exception.expect(AchillesException.class);
		exception.expectMessage("CQL does not support slice delete with varying compound components");

		cqlSliceQuery.validateSliceQueryForRemove();
	}

	@Test
	public void should_exception_when_limit_set_for_remove() throws Exception {
		when(sliceQuery.getClusteringsFrom()).thenReturn(Arrays.<Object> asList(11L));
		when(sliceQuery.getClusteringsTo()).thenReturn(Arrays.<Object> asList(11L));
		when(sliceQuery.isLimitSet()).thenReturn(true);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		exception.expect(AchillesException.class);
		exception.expectMessage("CQL slice delete does not support LIMIT");

		cqlSliceQuery.validateSliceQueryForRemove();
	}

	@Test
	public void should_get_entity_class() throws Exception {
		when(sliceQuery.getEntityClass()).thenReturn(ClusteredEntity.class);

		cqlSliceQuery = new CQLSliceQuery<ClusteredEntity>(sliceQuery, EACH_QUORUM);

		assertThat(cqlSliceQuery.getEntityClass()).isSameAs(ClusteredEntity.class);
	}
}
