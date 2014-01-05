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
package info.archinnov.achilles.entity.parsing.validator;

import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class EntityParsingValidatorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private EntityParsingValidator validator = new EntityParsingValidator();

	@Test
	public void should_exception_when_no_id_meta() throws Exception {
		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The entity '" + CompleteBean.class.getCanonicalName()
				+ "' should have at least one field with javax.persistence.Id/javax.persistence.EmbeddedId annotation");
		validator.validateHasIdMeta(CompleteBean.class, null);
	}
}
