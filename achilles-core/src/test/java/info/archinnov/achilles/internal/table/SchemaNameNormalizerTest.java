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
package info.archinnov.achilles.internal.table;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesInvalidTableException;

@RunWith(MockitoJUnitRunner.class)
public class SchemaNameNormalizerTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_exception_when_schema_name_exceeds_48_characters() throws Exception {
		String canonicalName = "ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters";

		exception.expect(AchillesInvalidTableException.class);
		exception.expectMessage("The schema name 'ItIsAVeryLoooooooooooooooooooooooooooooooooooooongClassNameExceeding48Characters' is invalid. It should respect the pattern [a-zA-Z0-9][a-zA-Z0-9_]{1,47} optionally enclosed in double quotes (\")");
		SchemaNameNormalizer.validateSchemaName(canonicalName);
	}

    @Test
    public void should_exception_when_schema_name_start_with_non_alpha_numeric() throws Exception {
        String canonicalName = "_test";

        exception.expect(AchillesInvalidTableException.class);
        exception.expectMessage("The schema name '_test' is invalid. It should respect the pattern [a-zA-Z0-9][a-zA-Z0-9_]{1,47} optionally enclosed in double quotes (\")");
        SchemaNameNormalizer.validateSchemaName(canonicalName);
    }

    @Test
    public void should_validate_correct_schema_name() throws Exception {
        SchemaNameNormalizer.validateSchemaName("complete_table");
    }

	@Test
	public void should_extract_table_name_from_canonical_name() throws Exception {
		String canonicalName = "org.achilles.entity.ClassName";

		String normalized = SchemaNameNormalizer.extractTableNameFromCanonical(canonicalName);

		assertThat(normalized).isEqualTo("ClassName");
	}
}
