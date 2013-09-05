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
package info.archinnov.achilles.table;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.exception.AchillesInvalidTableException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableNameNormalizerTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_exception_when_even_class_name_exceeeds_48_characters()
			throws Exception {
		String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

		exception.expect(AchillesInvalidTableException.class);
		exception
				.expectMessage("The table name 'ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters' is invalid. It should be respect the pattern [a-zA-Z0-9_] and be at most 48 characters long");
		TableNameNormalizer
				.normalizerAndValidateColumnFamilyName(canonicalName);
	}

	@Test
	public void should_normalize_canonical_classname() throws Exception {
		String canonicalName = "org.achilles.entity.ClassName";

		String normalized = TableNameNormalizer
				.normalizerAndValidateColumnFamilyName(canonicalName);

		assertThat(normalized).isEqualTo("ClassName");
	}
}
