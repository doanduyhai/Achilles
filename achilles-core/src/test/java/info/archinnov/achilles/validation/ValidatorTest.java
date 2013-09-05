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
package info.archinnov.achilles.validation;

import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.parser.entity.UserBean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ValidatorTest {

	@Test(expected = AchillesException.class)
	public void should_exception_when_blank() throws Exception {
		Validator.validateNotBlank("", "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_string_null() throws Exception {
		Validator.validateNotBlank(null, "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_null() throws Exception {
		Validator.validateNotNull(null, "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_empty_collection() throws Exception {
		Validator.validateNotEmpty(new ArrayList<String>(), "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_null_collection() throws Exception {
		Validator.validateNotEmpty((Collection<String>) null, "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_empty_map() throws Exception {
		Validator.validateNotEmpty(new HashMap<String, String>(), "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_null_map() throws Exception {
		Validator.validateNotEmpty((Map<String, String>) null, "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_incorrect_size_map() throws Exception {
		HashMap<String, String> map = new HashMap<String, String>();
		Validator.validateSize(map, 2, "arg", map);
	}

	@Test(expected = AchillesBeanMappingException.class)
	public void should_exception_when_no_default_constructor() throws Exception {
		Validator.validateNoargsConstructor(TestNoArgConstructor.class);
	}

	@Test
	public void should_match_pattern() throws Exception {
		Validator.validateRegExp("1_abcd01_sdf", "[a-zA-Z0-9_]+", "arg");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_not_matching_pattern() throws Exception {
		Validator.validateRegExp("1_a-bcd01_sdf", "[a-zA-Z0-9_]+", "arg");
	}

	@Test
	public void should_instanciate_a_bean() throws Exception {
		Validator.validateInstantiable(NormalClass.class);
	}

	@Test(expected = AchillesBeanMappingException.class)
	public void should_exception_when_private_class() throws Exception {
		Validator.validateInstantiable(PrivateEntity.class);
	}

	@Test(expected = AchillesBeanMappingException.class)
	public void should_exception_when_interface() throws Exception {
		Validator.validateInstantiable(TestInterface.class);
	}

	@Test(expected = AchillesBeanMappingException.class)
	public void should_exception_when_abstract_class() throws Exception {
		Validator.validateInstantiable(AbstractClass.class);
	}

	@Test(expected = AchillesBeanMappingException.class)
	public void should_exception_when_primitive() throws Exception {
		Validator.validateInstantiable(Long.class);
	}

	@Test(expected = AchillesBeanMappingException.class)
	public void should_exception_array_type() throws Exception {
		String[] array = new String[2];
		Validator.validateInstantiable(array.getClass());
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_map_size_does_not_match()
			throws Exception {
		HashMap<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "1");
		Validator.validateSize(map, 2, "");
	}

	@Test(expected = AchillesException.class)
	public void should_exception_when_not_implements_comparable()
			throws Exception {
		Validator.validateComparable(UserBean.class, "");

	}

	class TestNoArgConstructor {
		private TestNoArgConstructor() {
		}
	}
}
