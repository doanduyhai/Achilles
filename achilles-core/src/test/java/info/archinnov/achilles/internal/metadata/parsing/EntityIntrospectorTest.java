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
package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.ANY;
import static info.archinnov.achilles.type.ConsistencyLevel.LOCAL_QUORUM;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.THREE;
import static info.archinnov.achilles.type.ConsistencyLevel.TWO;
import static info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS;
import static info.archinnov.achilles.type.InsertStrategy.NOT_NULL_FIELDS;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.test.parser.entity.BeanWithKeyspaceAndTableName;
import info.archinnov.achilles.test.parser.entity.BeanWithNamingStrategy;
import info.archinnov.achilles.type.NamingStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.BeanWithComment;
import info.archinnov.achilles.test.parser.entity.ChildBean;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.type.Pair;

@RunWith(MockitoJUnitRunner.class)
public class EntityIntrospectorTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Mock
    private EntityMeta entityMeta;

    @Mock
    private PropertyMeta idMeta;

    @Mock
    private PropertyMeta wideMapMeta;

    @Mock
    private Map<Method, PropertyMeta> getterMetas;

    @Mock
    private Map<Method, PropertyMeta> setterMetas;

    @Mock
    private EntityParsingContext parsingContext;

    @Mock
    private ConfigurationContext configContext;

    private final EntityIntrospector introspector = new EntityIntrospector();

    @Test
    public void should_derive_getter() throws Exception {

        class Test {

            @SuppressWarnings("unused")
            Boolean old;
        }

        String[] getterNames = introspector.deriveGetterName(Test.class.getDeclaredField("old"));
        assertThat(getterNames).hasSize(1);
        assertThat(getterNames[0]).isEqualTo("getOld");
    }

    @Test
    public void should_derive_getter_for_boolean_primitive() throws Exception {

        class Test {

            @SuppressWarnings("unused")
            boolean old;
        }

        String[] getterNames = introspector.deriveGetterName(Test.class.getDeclaredField("old"));
        assertThat(getterNames).hasSize(2);
        assertThat(getterNames[0]).isEqualTo("isOld");
        assertThat(getterNames[1]).isEqualTo("getOld");
    }

    @Test
    public void should_derive_setter() throws Exception {
        class Test {
            @SuppressWarnings("unused")
            boolean a;
        }

        assertThat(introspector.deriveSetterName(Test.class.getDeclaredField("a"))).isEqualTo("setA");
    }

    @Test
    public void should_exception_when_no_getter() throws Exception {

        class Test {
            @SuppressWarnings("unused")
            String name;
        }

        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The getter for field 'name' of type 'null' does not exist");

        introspector.findGetter(Test.class, Test.class.getDeclaredField("name"));
    }

    @Test
    public void should_exception_when_no_setter() throws Exception {

        class Test {
            String name;

            @SuppressWarnings("unused")
            public String getA() {
                return name;
            }
        }

        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The setter for field 'name' of type 'null' does not exist");

        introspector.findSetter(Test.class, Test.class.getDeclaredField("name"));
    }

    @Test
    public void should_exception_when_incorrect_getter() throws Exception {

        class Test {
            @SuppressWarnings("unused")
            String name;

            @SuppressWarnings("unused")
            public Long getName() {
                return 1L;
            }

        }
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The getter for field 'name' of type 'null' does not return correct type");

        introspector.findGetter(Test.class, Test.class.getDeclaredField("name"));
    }

    @Test
    public void should_exception_when_setter_returning_wrong_type() throws Exception {

        @SuppressWarnings("unused")
        class Test {
            String name;

            public String getName() {
                return name;
            }

            public Long setName(String name) {
                return 1L;
            }

        }
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx
                .expectMessage("The setter for field 'name' of type 'null' does not return correct type or does not have the correct parameter");

        introspector.findSetter(Test.class, Test.class.getDeclaredField("name"));
    }

    @Test
    public void should_exception_when_setter_taking_wrong_type() throws Exception {

        @SuppressWarnings("unused")
        class Test {
            String name;

            public String getName() {
                return name;
            }

            public void setName(Long name) {
            }

        }

        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The setter for field 'name' of type 'null' does not exist or is incorrect");

        introspector.findSetter(Test.class, Test.class.getDeclaredField("name"));
    }

    @Test
    public void should_find_getter_from_boolean_as_isOld() throws Exception {
        @SuppressWarnings("unused")
        class Test {
            boolean old;

            public boolean isOld() {
                return old;
            }

            public void setOld(boolean old) {
                this.old = old;
            }
        }

        Method[] accessors = introspector.findAccessors(Test.class, Test.class.getDeclaredField("old"));

        assertThat(accessors[0].getName()).isEqualTo("isOld");
    }

    @Test
    public void should_find_getter_from_boolean_as_getOld() throws Exception {
        @SuppressWarnings("unused")
        class Test {
            boolean old;

            public boolean getOld() {
                return old;
            }

            public void setOld(boolean old) {
                this.old = old;
            }
        }

        Method[] accessors = introspector.findAccessors(Test.class, Test.class.getDeclaredField("old"));

        assertThat(accessors[0].getName()).isEqualTo("getOld");
    }

    @Test
    public void should_find_accessors() throws Exception {

        Method[] accessors = introspector.findAccessors(Bean.class,
                Bean.class.getDeclaredField("complicatedAttributeName"));

        assertThat(accessors).hasSize(2);
        assertThat(accessors[0].getName()).isEqualTo("getComplicatedAttributeName");
        assertThat(accessors[1].getName()).isEqualTo("setComplicatedAttributeName");
    }

    @Test
    public void should_find_accessors_from_collection_types() throws Exception {

        Method[] accessors = introspector.findAccessors(ComplexBean.class,
                ComplexBean.class.getDeclaredField("friends"));

        assertThat(accessors).hasSize(2);
        assertThat(accessors[0].getName()).isEqualTo("getFriends");
        assertThat(accessors[1].getName()).isEqualTo("setFriends");
    }

    @Test
    public void should_find_accessors_from_counter_type() throws Exception {
        Method[] accessors = introspector.findAccessors(CompleteBean.class,
                CompleteBean.class.getDeclaredField("count"));

        assertThat(accessors).hasSize(2);
        assertThat(accessors[0].getName()).isEqualTo("getCount");
        assertThat(accessors[1].getName()).isEqualTo("setCount");
    }

    @Test
    public void should_get_inherited_fields() throws Exception {
        List<Field> fields = introspector.getInheritedPrivateFields(ChildBean.class);

        assertThat(fields).hasSize(4);
        assertThat(fields.get(0).getName()).isEqualTo("nickname");
        assertThat(fields.get(1).getName()).isEqualTo("name");
        assertThat(fields.get(2).getName()).isEqualTo("address");
        assertThat(fields.get(3).getName()).isEqualTo("id");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_get_inherited_field_by_annotation() throws Exception {
        Field id = introspector.getInheritedPrivateFields(ChildBean.class, PartitionKey.class);

        assertThat(id.getName()).isEqualTo("id");
        assertThat(id.getType()).isEqualTo((Class) Long.class);
    }

    @Test
    public void should_not_get_inherited_field_by_annotation_when_no_match() throws Exception {
        assertThat(introspector.getInheritedPrivateFields(ChildBean.class, TimeUUID.class)).isNull();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_get_inherited_field_by_annotation_and_name() throws Exception {
        Field address = introspector.getInheritedPrivateFields(ChildBean.class, Column.class, "address");

        assertThat(address.getName()).isEqualTo("address");
        assertThat(address.getType()).isEqualTo((Class) String.class);
    }

    @Test
    public void should_not_get_inherited_field_by_annotation_and_name_when_no_match() throws Exception {
        assertThat(introspector.getInheritedPrivateFields(ChildBean.class, TimeUUID.class, "address")).isNull();
    }

    @Test
    public void should_infer_table_comment_from_annotation() throws Exception {
        String comment = introspector.inferTableComment(BeanWithComment.class, "default comment");
        assertThat(comment).isEqualTo("Table BeanWithComment");
    }

    @Test
    public void should_infer_table_comment_from_default_value() throws Exception {
        String comment = introspector.inferTableComment(BeanWithKeyspaceAndTableName.class, "default comment");
        assertThat(comment).isEqualTo("default comment");
    }

    @Test
    public void should_infer_keyspace_name_from_annotation() throws Exception {
        //When
        String keyspaceName = introspector.inferKeyspaceName(BeanWithNamingStrategy.class, Optional.<String>absent(), NamingStrategy.SNAKE_CASE);

        //Then
        assertThat(keyspaceName).isEqualTo("my_keyspace");
    }

    @Test
    public void should_infer_keyspace_name_from_config() throws Exception {
        //When
        String keyspaceName = introspector.inferKeyspaceName(ComplexBean.class, Optional.fromNullable("ks"), NamingStrategy.LOWER_CASE);

        //Then
        assertThat(keyspaceName).isEqualTo("ks");
    }

    @Test
    public void should_exception_when_keyspace_name_not_found() throws Exception {
        //When
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("No keyspace name found for entity '"+CompleteBean.class.getCanonicalName()+"'. Keyspace name is looked up using either the @Entity annotation or in configuration parameter");

        introspector.inferKeyspaceName(CompleteBean.class,  Optional.<String>absent(), NamingStrategy.LOWER_CASE);
    }

    @Test
    public void should_infer_table_name_from_annotation() throws Exception {
        String tableName = introspector.inferTableName(BeanWithNamingStrategy.class,  "canonicalName", NamingStrategy.SNAKE_CASE);
        assertThat(tableName).isEqualTo("my_table");
    }

    @Test
    public void should_infer_table_name_from_default_name() throws Exception {
        String tableName = introspector.inferTableName(CompleteBean.class, CompleteBean.class.getCanonicalName(), NamingStrategy.SNAKE_CASE);
        assertThat(tableName).isEqualTo("complete_bean");
    }

    @Test
    public void should_infer_table_name_from_default_name_when_empty_annotation_name() throws Exception {
        @Entity(table = "")
        class Test {

        }
        String tableName = introspector.inferTableName(Test.class, "canonicalName", NamingStrategy.LOWER_CASE);
        assertThat(tableName).isEqualTo("canonicalname");
    }

    


    @Test
    public void should_find_consistency_level_from_class() throws Exception {
        @Consistency(read = ANY, write = LOCAL_QUORUM)
        class Test {
        }

        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ONE);
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(TWO);
        when(configContext.getReadConsistencyLevelForTable("table")).thenReturn(null);
        when(configContext.getWriteConsistencyLevelForTable("table")).thenReturn(null);

        Pair<ConsistencyLevel, ConsistencyLevel> levels = introspector.findConsistencyLevels(Test.class,"table",configContext);

        assertThat(levels.left).isEqualTo(ANY);
        assertThat(levels.right).isEqualTo(LOCAL_QUORUM);
    }

    @Test
    public void should_find_consistency_level_by_default() throws Exception {
        class Test {
        }

        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ONE);
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(TWO);
        when(configContext.getReadConsistencyLevelForTable("table")).thenReturn(null);
        when(configContext.getWriteConsistencyLevelForTable("table")).thenReturn(null);

        Pair<ConsistencyLevel, ConsistencyLevel> levels = introspector.findConsistencyLevels(Test.class,"table",configContext);

        assertThat(levels.left).isEqualTo(ONE);
        assertThat(levels.right).isEqualTo(TWO);
    }

    @Test
    public void should_find_consistency_level_from_map_overriding_default() throws Exception {
        //Given
        class Test {
        }

        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ONE);
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(TWO);
        when(configContext.getReadConsistencyLevelForTable("table")).thenReturn(THREE);
        when(configContext.getWriteConsistencyLevelForTable("table")).thenReturn(ALL);

        //When
        Pair<ConsistencyLevel, ConsistencyLevel> levels = introspector.findConsistencyLevels(Test.class,"table",configContext);

        //Then
        assertThat(levels.left).isEqualTo(THREE);
        assertThat(levels.right).isEqualTo(ALL);
    }

    @Test
    public void should_find_consistency_level_from_map_overriding_entity() throws Exception {
        //Given
        @Consistency(read = ANY, write = LOCAL_QUORUM)
        class Test {
        }

        when(configContext.getDefaultReadConsistencyLevel()).thenReturn(ONE);
        when(configContext.getDefaultWriteConsistencyLevel()).thenReturn(TWO);
        when(configContext.getReadConsistencyLevelForTable("table")).thenReturn(THREE);
        when(configContext.getWriteConsistencyLevelForTable("table")).thenReturn(ALL);

        //When
        Pair<ConsistencyLevel, ConsistencyLevel> levels = introspector.findConsistencyLevels(Test.class,"table",configContext);

        //Then
        assertThat(levels.left).isEqualTo(THREE);
        assertThat(levels.right).isEqualTo(ALL);
    }

    @Test
    public void should_get_insert_strategy_on_entity() throws Exception {
        //Given
        when(parsingContext.getDefaultInsertStrategy()).thenReturn(ALL_FIELDS);

        //When
        final InsertStrategy insertStrategy = introspector.getInsertStrategy(ComplexBean.class, parsingContext);

        //Then
        assertThat(insertStrategy).isEqualTo(NOT_NULL_FIELDS);
    }

    @Test
    public void should_get_default_insert_strategy_on_entity() throws Exception {
        //Given
        when(parsingContext.getDefaultInsertStrategy()).thenReturn(ALL_FIELDS);

        //When
        final InsertStrategy insertStrategy = introspector.getInsertStrategy(Bean.class, parsingContext);

        //Then
        assertThat(insertStrategy).isEqualTo(ALL_FIELDS);
    }

    @Test
    public void should_determine_class_naming_strategy() throws Exception {
        //When
        when(configContext.getGlobalNamingStrategy()).thenReturn(NamingStrategy.LOWER_CASE);
        final NamingStrategy classNamingStrategy = introspector.determineClassNamingStrategy(configContext, BeanWithNamingStrategy.class);
        final NamingStrategy defaultNamingStrategy = introspector.determineClassNamingStrategy(configContext, CompleteBean.class);

        //Then
        assertThat(classNamingStrategy).isSameAs(NamingStrategy.SNAKE_CASE);
        assertThat(defaultNamingStrategy).isSameAs(NamingStrategy.LOWER_CASE);
    }

    @Test
    public void should_infer_property_name_from_class_naming_strategy() throws Exception {
        //Given
        Field field = BeanWithNamingStrategy.class.getDeclaredField("firstName");

        //When
        final String actual = introspector.inferCQLColumnName(field, NamingStrategy.SNAKE_CASE);

        //Then
        assertThat(actual).isEqualTo("first_name");
    }

    @Test
         public void should_infer_property_name_from_column_annotation() throws Exception {
        //Given
        Field field = BeanWithNamingStrategy.class.getDeclaredField("lastName");

        //When
        final String actual = introspector.inferCQLColumnName(field, NamingStrategy.SNAKE_CASE);

        //Then
        assertThat(actual).isEqualTo("\"lastName\"");
    }

    @Test
    public void should_infer_property_name_from_id_annotation() throws Exception {
        //Given
        Field field = BeanWithNamingStrategy.class.getDeclaredField("id");

        //When
        final String actual = introspector.inferCQLColumnName(field, NamingStrategy.SNAKE_CASE);

        //Then
        assertThat(actual).isEqualTo("my_Id");
    }

    @Test
    public void should_infer_property_name_to_lower_case_when_no_column_annotation() throws Exception {
        //Given
        Field field = BeanWithNamingStrategy.class.getDeclaredField("unMappedColumn");

        //When
        final String actual = introspector.inferCQLColumnName(field, NamingStrategy.LOWER_CASE);

        //Then
        assertThat(actual).isEqualTo("unmappedcolumn");
    }


    class Bean {

        private String complicatedAttributeName;

        public String getComplicatedAttributeName() {
            return complicatedAttributeName;
        }

        public void setComplicatedAttributeName(String complicatedAttributeName) {
            this.complicatedAttributeName = complicatedAttributeName;
        }
    }

    @Strategy(insert = InsertStrategy.NOT_NULL_FIELDS)
    class ComplexBean {
        private List<String> friends;

        public List<String> getFriends() {
            return friends;
        }

        public void setFriends(List<String> friends) {
            this.friends = friends;
        }
    }
}
