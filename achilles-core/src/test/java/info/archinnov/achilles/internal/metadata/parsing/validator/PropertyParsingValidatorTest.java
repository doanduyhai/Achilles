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
package info.archinnov.achilles.internal.metadata.parsing.validator;

import static info.archinnov.achilles.internal.metadata.holder.PropertyMetaTestBuilder.valueClass;
import static info.archinnov.achilles.internal.metadata.holder.PropertyType.SIMPLE;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.ANY;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.CorrectEmbeddedKey;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Pair;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParsingValidatorTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private PropertyParsingValidator validator = new PropertyParsingValidator();

    @Mock
    private PropertyParsingContext context;

    @Test
    public void should_exception_when_duplicate_property_name() throws Exception {
        Map<String, PropertyMeta> propertyMetas = new HashMap<>();
        propertyMetas.put("name", null);
        when(context.getCurrentPropertyName()).thenReturn("name");
        when(context.getPropertyMetas()).thenReturn(propertyMetas);
        when(context.<CompleteBean>getCurrentEntityClass()).thenReturn(CompleteBean.class);

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The property 'name' is already used for the entity '" + CompleteBean.class.getCanonicalName() + "'");

        validator.validateNoDuplicatePropertyName(context);
    }

    @Test
    public void should_exception_when_duplicate_cql3_name() throws Exception {
        PropertyMeta name = valueClass(String.class).cqlColumnName("name").type(SIMPLE).build();
        PropertyMeta duplicatedName = valueClass(String.class).cqlColumnName("name").type(SIMPLE).build();
        Map<String, PropertyMeta> propertyMetas = ImmutableMap.of("name1", name, "name2", duplicatedName);
        when(context.getCurrentPropertyName()).thenReturn("name");
        when(context.getCurrentCQL3ColumnName()).thenReturn("name");
        when(context.getPropertyMetas()).thenReturn(propertyMetas);
        when(context.<CompleteBean>getCurrentEntityClass()).thenReturn(CompleteBean.class);

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage("The CQL column 'name' is already used for the entity '" + CompleteBean.class.getCanonicalName() + "'");

        validator.validateNoDuplicateCQLName(context);
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

    @Test
    public void should_validate_consistency_level_for_counter() throws Exception {
        class Test {
            @SuppressWarnings("unused")
            public Long counter;
        }
        Field counterField = Test.class.getField("counter");
        when(context.getCurrentField()).thenReturn(counterField);
        when(context.<Test>getCurrentEntityClass()).thenReturn(Test.class);

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

    @Test
    public void should_validate_index() throws Exception {
        class Test {
            @Index
            public Long counter;
        }
        Field counterField = Test.class.getField("counter");
        when(context.getCurrentField()).thenReturn(counterField);
        when(context.<Test>getCurrentEntityClass()).thenReturn(Test.class);

        validator.validateIndexIfSet(context);
    }

    @Test
    public void should_validate_enum_as_index() throws Exception {
        class Test {
            @Index
            public ConsistencyLevel consistency;
        }
        Field consistencyField = Test.class.getField("consistency");
        when(context.getCurrentField()).thenReturn(consistencyField);
        when(context.<Test>getCurrentEntityClass()).thenReturn(Test.class);

        validator.validateIndexIfSet(context);
    }

//    @Test
//    public void should_exception_when_type_not_allowed() throws Exception {
//        Set<Class<?>> allowedTypes = new HashSet<>();
//        allowedTypes.add(Long.class);
//        allowedTypes.add(String.class);
//
//        exception.expect(AchillesBeanMappingException.class);
//        exception.expectMessage("msg1");
//
//        PropertyParsingValidator.validateAllowedTypes(Integer.class, allowedTypes, "msg1");
//
//        PropertyParsingValidator.validateAllowedTypes(CorrectEmbeddedKey.class, allowedTypes, "msg1");
//        PropertyParsingValidator.validateAllowedTypes(CustomEnum.class, allowedTypes, "msg1");
//    }

    @Test
    public void should_exception_when_index_type_not_allowed() throws Exception {
        class Test {
            @Index
            public Map<?, ?> firstName;
        }

        Field nameField = Test.class.getField("firstName");

        Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
        propertyMetas.put("firstName", null);
        when(context.getCurrentField()).thenReturn(nameField);
        when(context.getCurrentPropertyName()).thenReturn("firstName");
        when(context.getPropertyMetas()).thenReturn(propertyMetas);
        when(context.<Test>getCurrentEntityClass()).thenReturn(Test.class);

        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("Property 'firstName' of entity 'null' cannot be indexed because the type 'java.util.Map' is not supported");

        validator.validateIndexIfSet(context);
    }

    @Test
    public void should_exception_when_index_not_allowed_on_primary_key() throws Exception {
        class Test {
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
        when(context.<Test>getCurrentEntityClass()).thenReturn(Test.class);

        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("Property 'firstName' of entity 'null' is a primary key and therefore cannot be indexed");

        validator.validateIndexIfSet(context);
    }

    @Test
    public void should_exception_when_index_not_allowed_on_embedded_id() throws Exception {
        class Test {
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
        when(context.<Test>getCurrentEntityClass()).thenReturn(Test.class);

        exception.expect(AchillesBeanMappingException.class);
        exception
                .expectMessage("Property 'firstName' of entity 'null' is part of a compound primary key and therefore cannot be indexed");

        validator.validateIndexIfSet(context);
    }

    public static enum CustomEnum {
        ONE, TWO, THREE
    }
}
