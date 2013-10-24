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

import static info.archinnov.achilles.type.OrderingMode.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftSliceQueryValidatorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftSliceQueryValidator validator;

	@Mock
	private PropertyMeta pm;

	@Before
	public void setUp() {
		when(pm.getEntityClassName()).thenReturn("entityClass");
	}

	@Test
	public void should_validate_components_for_query_in_ascending_order_for_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 13);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 14);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		// ///////////////
		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("b", 13);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("b", 14);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("b", 13);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		// //////////////
		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("a", 14);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", null);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		// /////////////////
		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("b", null);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("b", 14);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("b", null);
		validator.validateComponentsForSliceQuery(start, end, ASCENDING);
	}

	@Test
	public void should_validate_components_for_query_in_descending_order_for_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 13);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", 13);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		// ///////////////
		start = Arrays.<Object> asList("b", 13);
		end = Arrays.<Object> asList("a", 13);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		start = Arrays.<Object> asList("b", 13);
		end = Arrays.<Object> asList("a", 14);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		start = Arrays.<Object> asList("b", 14);
		end = Arrays.<Object> asList("a", 13);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		// //////////////
		start = Arrays.<Object> asList("a", null);
		end = Arrays.<Object> asList("a", 14);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", null);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		// //////////////////
		start = Arrays.<Object> asList("b", null);
		end = Arrays.<Object> asList("a", null);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		start = Arrays.<Object> asList("b", null);
		end = Arrays.<Object> asList("a", 14);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);

		start = Arrays.<Object> asList("b", 14);
		end = Arrays.<Object> asList("a", null);
		validator.validateComponentsForSliceQuery(start, end, DESCENDING);
	}

	@Test
	public void should_exception_when_start_greater_than_end_for_ascending_for_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 14);
		end = Arrays.<Object> asList("a", 13);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query with ascending order, start component '14' should be lesser or equal to end component '13'");

		validator.validateComponentsForSliceQuery(start, end, ASCENDING);

	}

	@Test
	public void should_exception_when_start_lesser_than_end_for_descending_for_query() throws Exception {
		List<Object> start;
		List<Object> end;

		start = Arrays.<Object> asList("a", 13);
		end = Arrays.<Object> asList("a", 14);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("For slice query with descending order, start component '13' should be greater or equal to end component '14'");

		validator.validateComponentsForSliceQuery(start, end, DESCENDING);
	}

	@Test
	public void should_validate_compound_keys_for_query() throws Exception {
		List<Object> start = Arrays.<Object> asList(11L, "a");
		List<Object> end = Arrays.<Object> asList(11L, "b");
		validator.validateComponentsForSliceQuery(pm, start, end, ASCENDING);
	}

	// ////////////////////////////////////
}
