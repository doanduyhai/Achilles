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
package info.archinnov.achilles.compound;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class ThriftSliceQueryValidatorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftSliceQueryValidator validator;

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
	public void should_validate_components_for_query_in_ascending_order_for_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		// ///////////////
		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("b", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("b", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("b", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		// //////////////
		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("a", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		// /////////////////
		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("b", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("b", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("b", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_validate_components_for_query_in_descending_order_for_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		// ///////////////
		start = Arrays.<Object> asList("b", 13);
		end = Arrays.<Object> asList("a", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("b", 13);
		end = Arrays.<Object> asList("a", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("b", 14);
		end = Arrays.<Object> asList("a", 13);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		// //////////////
		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("a", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		// //////////////////
		start = Arrays.<Object> asList("b", null);
		end = Arrays.<Object> asList("a", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("b", null);
		end = Arrays.<Object> asList("a", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);

		start = Arrays.<Object> asList("b", 14);
		end = Arrays.<Object> asList("a", null);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_exception_when_start_greater_than_end_for_ascending_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", 13);

		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query with ascending order, start component '14' should be lesser or equal to end component '13'");

		validator.validateComponentsForSliceQuery(sliceQuery);

	}

	@Test
	public void should_exception_when_start_lesser_than_end_for_descending_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 14);
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.DESCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query with descending order, start component '13' should be greater or equal to end component '14'");

		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	@Test
	public void should_validate_compound_keys_for_query() throws Exception {
		List<Object> start = Arrays.<Object> asList(11L, "a");
		List<Object> end = Arrays.<Object> asList(11L, "b");
		sliceQuery = new SliceQuery<Object>(Object.class, meta, Arrays.<Object> asList(10L), start, end,
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 100, false);
		validator.validateComponentsForSliceQuery(sliceQuery);
	}

	// ////////////////////////////////////
}
