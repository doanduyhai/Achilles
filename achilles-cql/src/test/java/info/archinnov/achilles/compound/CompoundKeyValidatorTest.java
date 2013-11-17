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

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CompoundKeyValidatorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CompoundKeyValidator validator = new CompoundKeyValidator() {
	};

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
}
