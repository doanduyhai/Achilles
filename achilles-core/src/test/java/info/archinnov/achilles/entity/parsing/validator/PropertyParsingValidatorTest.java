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

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.Lazy;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.CorrectCompoundKey;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EmbeddedId;
import javax.persistence.Id;

import org.apache.cassandra.utils.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParsingValidatorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private PropertyParsingValidator validator = new PropertyParsingValidator();

	@Mock
	private PropertyParsingContext context;

	@Test
	public void should_exception_when_duplicate_property_meta() throws Exception {
		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("name", null);
		when(context.getCurrentPropertyName()).thenReturn("name");
		when(context.getPropertyMetas()).thenReturn(propertyMetas);
		when((Class<CompleteBean>) context.getCurrentEntityClass()).thenReturn(CompleteBean.class);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The property 'name' is already used for the entity '"
				+ CompleteBean.class.getCanonicalName() + "'");

		validator.validateNoDuplicate(context);
	}

	@Test
	public void should_exception_when_map_not_parameterized() throws Exception {
		class Test {
			@SuppressWarnings({ "rawtypes", "unused" })
			public Map map;
		}

		Field mapField = Test.class.getField("map");

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The Map type should be parameterized for the entity '" + Test.class.getCanonicalName()
				+ "'");

		validator.validateMapGenerics(mapField, Test.class);
	}

	@Test
	public void should_exception_when_missing_parameter_for_map() throws Exception {
		class Test {
			@SuppressWarnings("unused")
			public List<String> map;
		}

		Field mapField = Test.class.getField("map");

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The Map type should be parameterized with <K,V> for the entity '"
				+ Test.class.getCanonicalName() + "'");

		validator.validateMapGenerics(mapField, Test.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_exception_when_missing_parameter_for_widemap() throws Exception {
		class Test {
			@SuppressWarnings("unused")
			public List<String> wideMap;
		}

		Field wideMapField = Test.class.getField("wideMap");

		when(context.getCurrentField()).thenReturn(wideMapField);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);
		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("The WideMap type should be parameterized with <K,V> for the entity '"
				+ Test.class.getCanonicalName() + "'");

		validator.validateWideMapGenerics(context);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate_consistency_level_for_counter() throws Exception {
		class Test {
			@SuppressWarnings("unused")
			public Long counter;
		}
		Field counterField = Test.class.getField("counter");
		when(context.getCurrentField()).thenReturn(counterField);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);

		Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels = Pair.create(ANY, ALL);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("Counter field 'counter' of entity '" + Test.class.getCanonicalName()
				+ "' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");

		validator.validateConsistencyLevelForCounter(context, consistencyLevels);

		consistencyLevels = Pair.create(ALL, ANY);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("Counter field 'counter' of entity '" + Test.class.getCanonicalName()
				+ "' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");

		validator.validateConsistencyLevelForCounter(context, consistencyLevels);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_validate_index() throws Exception {
		class Test {
			@SuppressWarnings("unused")
			@Index
			public Long counter;
		}
		Field counterField = Test.class.getField("counter");
		when(context.getCurrentField()).thenReturn(counterField);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);

		validator.validateIndexIfSet(context);
	}

	@Test
	public void should_exception_when_type_not_allowed() throws Exception {
		Set<Class<?>> allowedTypes = new HashSet<Class<?>>();
		allowedTypes.add(Long.class);
		allowedTypes.add(String.class);

		exception.expect(AchillesBeanMappingException.class);
		exception.expectMessage("msg1");

		PropertyParsingValidator.validateAllowedTypes(Integer.class, allowedTypes, "msg1");

		PropertyParsingValidator.validateAllowedTypes(CorrectCompoundKey.class, allowedTypes, "msg1");
		PropertyParsingValidator.validateAllowedTypes(CustomEnum.class, allowedTypes, "msg1");
	}

	@Test
	public void should_exception_when_index_type_not_allowed() throws Exception {
		class Test {
			@SuppressWarnings({ "rawtypes", "unused" })
			@Index
			public Map firstName;
		}

		Field nameField = Test.class.getField("firstName");

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("firstName", null);
		when(context.getCurrentField()).thenReturn(nameField);
		when(context.getCurrentPropertyName()).thenReturn("firstName");
		when(context.getPropertyMetas()).thenReturn(propertyMetas);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("Property field 'firstName' of entity 'null' cannot have an index annotation (class not supported)");

		validator.validateIndexIfSet(context);
	}

	@Test
	public void should_exception_when_index_not_allowed_on_primary_key() throws Exception {
		class Test {
			@SuppressWarnings({ "rawtypes", "unused" })
			@Id
			@Index
			public String firstName;
		}

		Field nameField = Test.class.getField("firstName");

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("firstName", null);
		when(context.getCurrentField()).thenReturn(nameField);
		when(context.getCurrentPropertyName()).thenReturn("firstName");
		when(context.getPropertyMetas()).thenReturn(propertyMetas);
		when(context.isPrimaryKey()).thenReturn(true);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("Property field 'firstName' of entity 'null' is a primary key and therefore cannot have an index annotation");

		validator.validateIndexIfSet(context);
	}

	@Test
	public void should_exception_when_index_not_allowed_on_embedded_id() throws Exception {
		class Test {
			@SuppressWarnings({ "rawtypes", "unused" })
			@EmbeddedId
			@Index
			public String firstName;
		}

		Field nameField = Test.class.getField("firstName");

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("firstName", null);
		when(context.getCurrentField()).thenReturn(nameField);
		when(context.getCurrentPropertyName()).thenReturn("firstName");
		when(context.getPropertyMetas()).thenReturn(propertyMetas);
		when(context.isPrimaryKey()).thenReturn(true);
		when(context.isEmbeddedId()).thenReturn(true);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("Property field 'firstName' of entity 'null' is part of the primary key (embedded key) and therefore cannot have an index annotation");

		validator.validateIndexIfSet(context);
	}

	@Test
	public void should_exception_when_index_not_allowed_lazy_field() throws Exception {
		class Test {
			@SuppressWarnings({ "rawtypes", "unused" })
			@Lazy
			@Index
			public String firstName;
		}

		Field nameField = Test.class.getField("firstName");

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("firstName", null);
		when(context.getCurrentField()).thenReturn(nameField);
		when(context.getCurrentPropertyName()).thenReturn("firstName");
		when(context.getPropertyMetas()).thenReturn(propertyMetas);
		when((Class<Test>) context.getCurrentEntityClass()).thenReturn(Test.class);

		exception.expect(AchillesBeanMappingException.class);
		exception
				.expectMessage("Property field 'firstName' of entity 'null' is lazy and therefore cannot have an index annotation");

		validator.validateIndexIfSet(context);
	}

	public static enum CustomEnum {
		ONE, TWO, THREE
	}
}
