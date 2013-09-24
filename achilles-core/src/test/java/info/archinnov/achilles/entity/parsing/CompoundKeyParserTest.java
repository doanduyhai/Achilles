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
package info.archinnov.achilles.entity.parsing;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.parser.entity.CompoundKeyIncorrectType;
import info.archinnov.achilles.test.parser.entity.CompoundKeyNotInstantiable;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithDuplicateOrder;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithNegativeOrder;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithNoAnnotation;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithOnlyOneComponent;
import info.archinnov.achilles.test.parser.entity.CompoundKeyWithTimeUUID;
import info.archinnov.achilles.test.parser.entity.CorrectCompoundKey;

import java.lang.reflect.Method;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CompoundKeyParserTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private CompoundKeyParser parser;

	@Test
	public void should_parse_compound_key() throws Exception {
		Method nameGetter = CorrectCompoundKey.class.getMethod("getName");
		Method nameSetter = CorrectCompoundKey.class.getMethod("setName",
				String.class);

		Method rankGetter = CorrectCompoundKey.class.getMethod("getRank");
		Method rankSetter = CorrectCompoundKey.class.getMethod("setRank",
				int.class);

		EmbeddedIdProperties props = parser
				.parseEmbeddedId(CorrectCompoundKey.class);

		assertThat(props.getComponentGetters()).containsExactly(nameGetter,
				rankGetter);
		assertThat(props.getComponentSetters()).containsExactly(nameSetter,
				rankSetter);
		assertThat(props.getComponentClasses()).containsExactly(String.class,
				int.class);
		assertThat(props.getComponentNames()).containsExactly("name", "rank");
		assertThat(props.getOrderingComponent()).isEqualTo("rank");
		assertThat(props.getClusteringComponentNames()).containsExactly("rank");
		assertThat(props.getClusteringComponentClasses()).containsExactly(
				int.class);
	}

	@Test
	public void should_parse_compound_key_with_time_uuid() throws Exception {
		EmbeddedIdProperties props = parser
				.parseEmbeddedId(CompoundKeyWithTimeUUID.class);

		assertThat(props.getTimeUUIDComponents()).containsExactly("date");
		assertThat(props.getComponentNames())
				.containsExactly("date", "ranking");
	}

	@Test
	public void should_exception_when_compound_key_incorrect_type()
			throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The class 'java.util.List' is not a valid component type for the @CompoundKey class '"
						+ CompoundKeyIncorrectType.class.getCanonicalName()
						+ "'");

		parser.parseEmbeddedId(CompoundKeyIncorrectType.class);
	}

	@Test
	public void should_exception_when_compound_key_wrong_key_order()
			throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("The key orders is wrong for @CompoundKey class '"
						+ CompoundKeyWithNegativeOrder.class.getCanonicalName()
						+ "'");

		parser.parseEmbeddedId(CompoundKeyWithNegativeOrder.class);
	}

	@Test
	public void should_exception_when_compound_key_has_no_annotation()
			throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("There should be at least 2 fields annotated with @Order for the compound primary key class '"
						+ CompoundKeyWithNoAnnotation.class.getCanonicalName()
						+ "'");

		parser.parseEmbeddedId(CompoundKeyWithNoAnnotation.class);
	}

	@Test
	public void should_exception_when_compound_key_has_duplicate_order()
			throws Exception {
		exception.expect(AchillesBeanMappingException.class);

		exception
				.expectMessage("The order '1' is duplicated in @CompoundKey class '"
						+ CompoundKeyWithDuplicateOrder.class
								.getCanonicalName() + "'");

		parser.parseEmbeddedId(CompoundKeyWithDuplicateOrder.class);
	}

	@Test
	public void should_exception_when_compound_key_no_pulic_default_constructor()
			throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The @CompoundKey class '"
				+ CompoundKeyNotInstantiable.class.getCanonicalName()
				+ "' should have a public default constructor");

		parser.parseEmbeddedId(CompoundKeyNotInstantiable.class);
	}

	@Test
	public void should_exception_when_compound_key_has_only_one_component()
			throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("There should be at least 2 fields annotated with @Order for the compound primary key class '"
						+ CompoundKeyWithOnlyOneComponent.class
								.getCanonicalName() + "'");
		parser.parseEmbeddedId(CompoundKeyWithOnlyOneComponent.class);
	}

}
