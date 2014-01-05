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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryValidatorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private SliceQueryValidator validator = new SliceQueryValidator();

	@Mock
	private PropertyMeta idMeta;

	@Mock
	private EntityMeta meta;

	private SliceQuery<Object> sliceQuery;

	@Before
	public void setUp() {
		when(meta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.encodeToComponents(anyListOf(Object.class))).thenAnswer(new Answer<List<Object>>() {
			@SuppressWarnings("unchecked")
			@Override
			public List<Object> answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				return (List<Object>) args[0];
			}
		});
	}

    @Test
    public void should_return_last_non_null_index_when_all_components_not_null() throws Exception {
        assertThat(validator.getLastNonNullIndex(Arrays.<Object> asList(11L, "name", 12.0))).isEqualTo(2);
    }

    @Test
    public void should_return_last_non_null_index_when_some_components_are_null() throws Exception {
        assertThat(validator.getLastNonNullIndex(Arrays.<Object> asList(11L, null, null))).isEqualTo(0);
    }

    @Test
    public void should_return_last_non_null_index_when_hole_in_component() throws Exception {
        assertThat(validator.getLastNonNullIndex(Arrays.<Object> asList(11L, null, 12))).isEqualTo(0);
    }

	@Test
	public void should_exception_when_components_not_in_correct_order_for_ascending() throws Exception {
		List<Object> clusteringsFrom = Arrays.<Object> asList(11);
		List<Object> clusteringsTo = Arrays.<Object> asList(10);
		OrderingMode ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query with ascending order, start clustering last component should be 'lesser or equal' to end clustering last component: [[11],[10]");
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_exception_when_components_not_in_correct_order_for_descending() throws Exception {
		List<Object> clusteringsFrom = Arrays.<Object> asList(11);
		List<Object> clusteringsTo = Arrays.<Object> asList(12);
		OrderingMode ordering = OrderingMode.DESCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query with descending order, start clustering last component should be 'greater or equal' to end clustering last component: [[11],[12]");
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_validate_clustering_components() throws Exception {
		UUID uuid1 = new UUID(10, 11);
		UUID uuid2 = new UUID(10, 12);
		OrderingMode ordering = OrderingMode.ASCENDING;

		List<Object> clusteringsFrom = Arrays.<Object> asList(uuid1, "author", 3);
		List<Object> clusteringsTo = Arrays.<Object> asList(uuid1, "author", 4);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, "author", 3);
		clusteringsTo = Arrays.<Object> asList(uuid1, "author", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, "author", null);
		clusteringsTo = Arrays.<Object> asList(uuid1, "author", 3);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, "a", null);
		clusteringsTo = Arrays.<Object> asList(uuid1, "b", null);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, "a", null);
		clusteringsTo = Arrays.<Object> asList(uuid1, null, null);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, null, null);
		clusteringsTo = Arrays.<Object> asList(uuid1, "b", null);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, null, null);
		clusteringsTo = Arrays.<Object> asList(uuid2, null, null);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(uuid1, "a", null);
		clusteringsTo = Arrays.<Object> asList(uuid1, "a", null);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(10L);
		clusteringsTo = Arrays.<Object> asList(10L);
		ordering = OrderingMode.ASCENDING;
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList(10L);
		clusteringsTo = Arrays.<Object> asList();
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		clusteringsFrom = Arrays.<Object> asList();
		clusteringsTo = Arrays.<Object> asList(10L);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_exception_when_too_many_start_components() throws Exception {
		UUID uuid1 = new UUID(10, 11);

		List<Object> clusteringsFrom = Arrays.<Object> asList(uuid1, "a", 1);
		List<Object> clusteringsTo = Arrays.<Object> asList(uuid1, null, null);
		OrderingMode ordering = OrderingMode.ASCENDING;

		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);

		exception.expect(AchillesException.class);
		exception.expectMessage("There should be no more than 1 component difference between clustering keys: [["
				+ uuid1 + ",a,1],[" + uuid1 + ",,]");
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_exception_when_components_are_not_equal_case1() throws Exception {
		UUID uuid1 = new UUID(10, 11);
		List<Object> clusteringsFrom = Arrays.<Object> asList(uuid1, "a", 1);
		List<Object> clusteringsTo = Arrays.<Object> asList(uuid1, "b", 2);
		OrderingMode ordering = OrderingMode.ASCENDING;

		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);

		exception.expect(AchillesException.class);
		exception.expectMessage("2th component for clustering keys should be equal: [[" + uuid1 + ",a,1],[" + uuid1
				+ ",b,2]");
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_exception_when_components_are_not_equal_case2() throws Exception {
		UUID uuid1 = new UUID(10, 11);

		List<Object> clusteringsFrom = Arrays.<Object> asList(uuid1, "a", null);
		List<Object> clusteringsTo = Arrays.<Object> asList(uuid1, "b", 2);
		OrderingMode ordering = OrderingMode.ASCENDING;

		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), clusteringsFrom,
				clusteringsTo, ordering, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);

		exception.expect(AchillesException.class);
		exception.expectMessage("2th component for clustering keys should be equal: [[" + uuid1 + ",a,],[" + uuid1
				+ ",b,2]");
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

}
