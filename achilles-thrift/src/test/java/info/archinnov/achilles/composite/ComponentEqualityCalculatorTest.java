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
package info.archinnov.achilles.composite;

import static info.archinnov.achilles.type.BoundingMode.*;
import static info.archinnov.achilles.type.OrderingMode.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.junit.Test;

public class ComponentEqualityCalculatorTest {
	private ComponentEqualityCalculator calculator = new ComponentEqualityCalculator();

	// Ascending order
	@Test
	public void should_return_determine_equalities_for_inclusive_start_and_end_asc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				INCLUSIVE_BOUNDS, ASCENDING);
		assertThat(equality[0]).isEqualTo(EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_and_end_asc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				EXCLUSIVE_BOUNDS, ASCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_inclusive_start_exclusive_end_asc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				INCLUSIVE_START_BOUND_ONLY, ASCENDING);
		assertThat(equality[0]).isEqualTo(EQUAL);
		assertThat(equality[1]).isEqualTo(LESS_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_inclusive_end_asc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				INCLUSIVE_END_BOUND_ONLY, ASCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	// Descending order
	@Test
	public void should_return_determine_equalities_for_inclusive_start_and_end_desc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				INCLUSIVE_BOUNDS, DESCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_and_end_desc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				EXCLUSIVE_BOUNDS, DESCENDING);
		assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_inclusive_start_exclusive_end_desc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				INCLUSIVE_START_BOUND_ONLY, DESCENDING);
		assertThat(equality[0]).isEqualTo(GREATER_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(GREATER_THAN_EQUAL);
	}

	@Test
	public void should_return_determine_equalities_for_exclusive_start_inclusive_end_desc()
			throws Exception {
		ComponentEquality[] equality = calculator.determineEquality(
				INCLUSIVE_END_BOUND_ONLY, DESCENDING);
		assertThat(equality[0]).isEqualTo(LESS_THAN_EQUAL);
		assertThat(equality[1]).isEqualTo(EQUAL);
	}
}
