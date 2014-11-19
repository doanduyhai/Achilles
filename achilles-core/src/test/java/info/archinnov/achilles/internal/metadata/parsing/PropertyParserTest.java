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

import static info.archinnov.achilles.internal.metadata.parsing.PropertyParser.isAssignableFromNativeType;
import static info.archinnov.achilles.internal.metadata.parsing.PropertyParser.isSupportedNativeType;
import static info.archinnov.achilles.internal.metadata.parsing.PropertyParser.isSupportedType;
import static info.archinnov.achilles.type.ConsistencyLevel.ALL;
import static info.archinnov.achilles.type.ConsistencyLevel.ANY;
import static info.archinnov.achilles.type.ConsistencyLevel.ONE;
import static info.archinnov.achilles.type.ConsistencyLevel.THREE;
import static info.archinnov.achilles.type.ConsistencyLevel.TWO;
import static org.fest.assertions.api.Assertions.assertThat;

import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.validation.constraints.NotNull;

import com.google.common.base.Optional;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.Enumerated.Encoding;
import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.type.NamingStrategy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Consistency;
import info.archinnov.achilles.annotations.EmbeddedId;
import info.archinnov.achilles.annotations.EmptyCollectionIfNull;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.annotations.Index;
import info.archinnov.achilles.annotations.TimeUUID;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;

@RunWith(MockitoJUnitRunner.class)
public class PropertyParserTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private PropertyParser parser = new PropertyParser();

    @Mock
    private ReflectionInvoker invoker;

    @Mock
    private List<Method> componentGetters;

    private EntityParsingContext entityContext;
    private ConfigurationContext configContext;

    @Before
    public void setUp() {
        configContext = new ConfigurationContext();
        configContext.setDefaultReadConsistencyLevel(ConsistencyLevel.ONE);
        configContext.setDefaultWriteConsistencyLevel(ConsistencyLevel.ALL);
        configContext.setCurrentKeyspace(Optional.fromNullable("ks"));
        configContext.setGlobalNamingStrategy(NamingStrategy.LOWER_CASE);
    }

    @Test
    public void should_determine_allowed_native_types() throws Exception {
        assertThat(isSupportedNativeType(byte.class)).isTrue();
        assertThat(isSupportedNativeType(Byte.class)).isTrue();
        assertThat(isSupportedNativeType(byte[].class)).isTrue();
        assertThat(isSupportedNativeType(ByteBuffer.class)).isTrue();
        assertThat(isSupportedNativeType(Boolean.class)).isTrue();
        assertThat(isSupportedNativeType(boolean.class)).isTrue();
        assertThat(isSupportedNativeType(Date.class)).isTrue();
        assertThat(isSupportedNativeType(Double.class)).isTrue();
        assertThat(isSupportedNativeType(double.class)).isTrue();
        assertThat(isSupportedNativeType(BigDecimal.class)).isTrue();
        assertThat(isSupportedNativeType(Float.class)).isTrue();
        assertThat(isSupportedNativeType(float.class)).isTrue();
        assertThat(isSupportedNativeType(InetAddress.class)).isTrue();
        assertThat(isSupportedNativeType(BigInteger.class)).isTrue();
        assertThat(isSupportedNativeType(Integer.class)).isTrue();
        assertThat(isSupportedNativeType(int.class)).isTrue();
        assertThat(isSupportedNativeType(Long.class)).isTrue();
        assertThat(isSupportedNativeType(long.class)).isTrue();
        assertThat(isSupportedNativeType(String.class)).isTrue();
        assertThat(isSupportedNativeType(UUID.class)).isTrue();
        assertThat(isSupportedNativeType(Object.class)).isFalse();
    }

    @Test
    public void should_determine_assignabled_from_native_types() throws Exception {
        assertThat(isAssignableFromNativeType(byte.class)).isTrue();
        assertThat(isAssignableFromNativeType(Byte.class)).isTrue();
        assertThat(isAssignableFromNativeType(byte[].class)).isTrue();
        assertThat(isAssignableFromNativeType(ByteBuffer.wrap("entityValue".getBytes()).getClass())).isTrue();
        assertThat(isAssignableFromNativeType(Boolean.class)).isTrue();
        assertThat(isAssignableFromNativeType(boolean.class)).isTrue();
        assertThat(isAssignableFromNativeType(Date.class)).isTrue();
        assertThat(isAssignableFromNativeType(Double.class)).isTrue();
        assertThat(isAssignableFromNativeType(double.class)).isTrue();
        assertThat(isAssignableFromNativeType(BigDecimal.class)).isTrue();
        assertThat(isAssignableFromNativeType(Float.class)).isTrue();
        assertThat(isAssignableFromNativeType(float.class)).isTrue();
        assertThat(isAssignableFromNativeType(InetAddress.class)).isTrue();
        assertThat(isAssignableFromNativeType(BigInteger.class)).isTrue();
        assertThat(isAssignableFromNativeType(Integer.class)).isTrue();
        assertThat(isAssignableFromNativeType(int.class)).isTrue();
        assertThat(isAssignableFromNativeType(Long.class)).isTrue();
        assertThat(isAssignableFromNativeType(long.class)).isTrue();
        assertThat(isAssignableFromNativeType(String.class)).isTrue();
        assertThat(isAssignableFromNativeType(UUID.class)).isTrue();
        assertThat(isAssignableFromNativeType(Object.class)).isFalse();
    }

    @Test
    public void should_determine_allowed_types_null_safe() throws Exception {
        assertThat(isSupportedType(null)).isFalse();
        assertThat(isSupportedType(ConsistencyLevel.class)).isTrue();
        assertThat(isSupportedType(String.class)).isTrue();
    }


    @Test
    public void should_parse_primary_key() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Id
            private Long id;

            public Long getId() {
                return id;
            }

            public void setId(Long id) {
                this.id = id;
            }
        }

        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("id"));
        context.setPrimaryKey(true);

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getPropertyName()).isEqualTo("id");
        assertThat(meta.<Long>getValueClass()).isEqualTo(Long.class);
        assertThat(context.getPropertyMetas()).hasSize(1);

    }


    @SuppressWarnings("unchecked")
    @Test
    public void should_parse_simple_property_string() throws Exception {

        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            private String name;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }

        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("name"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getPropertyName()).isEqualTo("name");
        assertThat(meta.<String>getValueClass()).isEqualTo(String.class);

        assertThat(meta.getGetter().getName()).isEqualTo("getName");
        assertThat((Class<String>) meta.getGetter().getReturnType()).isEqualTo(String.class);
        assertThat(meta.getSetter().getName()).isEqualTo("setName");
        assertThat((Class<String>) meta.getSetter().getParameterTypes()[0]).isEqualTo(String.class);

        assertThat(meta.type()).isEqualTo(PropertyType.SIMPLE);

        assertThat(context.getPropertyMetas().get("name")).isSameAs(meta);

    }

    @Test
    public void should_parse_id_property_and_override_name() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Id(name = "my_custom_id")
            private Long customId;

            public Long getCustomId() {
                return customId;
            }

            public void setCustomId(Long customId) {
                this.customId = customId;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("customId"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getCQL3ColumnName()).isEqualTo("my_custom_id");
    }

    @Test
    public void should_parse_simple_property_and_override_name() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column(name = "firstname")
            private String name;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("name"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getCQL3ColumnName()).isEqualTo("firstname");
    }

    @Test
    public void should_parse_simple_property_with_snake_case_naming_strategy() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        @Strategy(naming = NamingStrategy.SNAKE_CASE)
        class Test {
            @Column
            private String firstName;

            public String getFirstName() {
                return firstName;
            }

            public void setFirstName(String name) {
                this.firstName = name;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("firstName"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getCQL3ColumnName()).isEqualTo("first_name");
    }


    @SuppressWarnings("unchecked")
    @Test
    public void should_parse_simple_static_property_string() throws Exception {

        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column(staticColumn = true)
            private String name;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }

        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("name"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.structure().isStaticColumn()).isTrue();
    }

    @Test
    public void should_parse_simple_property_of_time_uuid_type() throws Exception {

        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @TimeUUID
            @Column
            private UUID date;

            public UUID getDate() {
                return date;
            }

            public void setDate(UUID date) {
                this.date = date;
            }
        }

        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("date"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.structure().isTimeUUID()).isTrue();
    }

    @Test
    public void should_parse_primitive_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            private boolean active;

            public boolean isActive() {
                return active;
            }

            public void setActive(boolean active) {
                this.active = active;
            }

        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("active"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.<Boolean>getValueClass()).isEqualTo(boolean.class);
    }

    @Test
    public void should_parse_counter_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            private Counter counter;

            public Counter getCounter() {
                return counter;
            }

            public void setCounter(Counter counter) {
                this.counter = counter;
            }

        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("counter"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
        assertThat(meta.getCounterProperties()).isNotNull();
        assertThat(meta.getCounterProperties().getFqcn()).isEqualTo(Test.class.getCanonicalName());
        assertThat(context.getCounterMetas().get(0)).isSameAs(meta);
    }

    @Test
    public void should_parse_static_counter_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column(staticColumn = true)
            private Counter counter;

            public Counter getCounter() {
                return counter;
            }

            public void setCounter(Counter counter) {
                this.counter = counter;
            }

        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("counter"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.structure().isStaticColumn()).isTrue();
    }

    @Test
    public void should_parse_counter_property_with_consistency_level() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Consistency(read = ONE, write = ALL)
            @Column
            private Counter counter;

            public Counter getCounter() {
                return counter;
            }

            public void setCounter(Counter counter) {
                this.counter = counter;
            }
        }
        entityContext = new EntityParsingContext(configContext, Test.class);
        entityContext.setCurrentConsistencyLevels(Pair.create(TWO, THREE));
        PropertyParsingContext context = entityContext.newPropertyContext(Test.class.getDeclaredField("counter"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.type()).isEqualTo(PropertyType.COUNTER);
        assertThat(meta.config().getReadConsistencyLevel()).isEqualTo(ONE);
        assertThat(meta.config().getWriteConsistencyLevel()).isEqualTo(ALL);
    }

    @Test
    public void should_exception_when_counter_consistency_is_any_for_read() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Consistency(read = ANY, write = ALL)
            @Column
            private Counter counter;

            public Counter getCounter() {
                return counter;
            }

            public void setCounter(Counter counter) {
                this.counter = counter;
            }
        }
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("Counter field 'counter' of entity 'null' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
        entityContext = new EntityParsingContext(configContext, Test.class);
        entityContext.setCurrentConsistencyLevels(Pair.create(TWO, THREE));
        PropertyParsingContext context = entityContext.newPropertyContext(Test.class.getDeclaredField("counter"));
        parser.parse(context);
    }

    @Test
    public void should_exception_when_counter_consistency_is_any_for_write() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Consistency(read = ONE, write = ANY)
            @Column
            private Counter counter;

            public Counter getCounter() {
                return counter;
            }

            public void setCounter(Counter counter) {
                this.counter = counter;
            }
        }

        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("Counter field 'counter' of entity 'null' cannot have ANY as read/write consistency level. All consistency levels except ANY are allowed");
        entityContext = new EntityParsingContext(configContext, Test.class);
        entityContext.setCurrentConsistencyLevels(Pair.create(TWO, THREE));
        PropertyParsingContext context = entityContext.newPropertyContext(Test.class.getDeclaredField("counter"));
        parser.parse(context);
    }

    @Test
    public void should_parse_enum_by_name_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            private PropertyType type;

            public PropertyType getType() {
                return type;
            }

            public void setType(PropertyType type) {
                this.type = type;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("type"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.<PropertyType>getValueClass()).isEqualTo(PropertyType.class);
        assertThat(meta.structure().<String>getCQL3ValueType()).isEqualTo(String.class);
    }

    @Test
    public void should_parse_enum_by_ordinal_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            @Enumerated(Encoding.ORDINAL)
            private PropertyType type;

            public PropertyType getType() {
                return type;
            }

            public void setType(PropertyType type) {
                this.type = type;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("type"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.<PropertyType>getValueClass()).isEqualTo(PropertyType.class);
        assertThat(meta.structure().<Integer>getCQL3ValueType()).isEqualTo(Integer.class);
    }

    @Test
    public void should_parse_enum_list_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            @Enumerated(Encoding.ORDINAL)
            private List<PropertyType> types;

            public List<PropertyType> getTypes() {
                return types;
            }

            public void setTypes(List<PropertyType> types) {
                this.types = types;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("types"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.<PropertyType>getValueClass()).isEqualTo(PropertyType.class);
        assertThat(meta.structure().<Integer>getCQL3ValueType()).isEqualTo(Integer.class);
    }

    @Test
    public void should_parse_enum_map_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            @Enumerated(key = Encoding.ORDINAL, value = Encoding.NAME)
            private Map<RetentionPolicy, PropertyType> types;

            public Map<RetentionPolicy, PropertyType> getTypes() {
                return types;
            }

            public void setTypes(Map<RetentionPolicy, PropertyType> types) {
                this.types = types;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("types"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.<PropertyType>getValueClass()).isEqualTo(PropertyType.class);
        assertThat(meta.structure().<Integer>getCQL3KeyType()).isEqualTo(Integer.class);
        assertThat(meta.structure().<String>getCQL3ValueType()).isEqualTo(String.class);
    }


    @Test
    public void should_parse_allowed_type_property() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            private UUID uuid;

            public UUID getUuid() {
                return uuid;
            }

            public void setUuid(UUID uuid) {
                this.uuid = uuid;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("uuid"));

        PropertyMeta meta = parser.parse(context);

        assertThat(meta.<UUID>getValueClass()).isEqualTo(UUID.class);
    }

    @Test
    public void should_parse_index() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            @Index
            private String firstname;

            public String getFirstname() {
                return firstname;
            }

            public void setFirstname(String firstname) {
                this.firstname = firstname;
            }

        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("firstname"));
        PropertyMeta meta = parser.parse(context);
        assertThat(meta.structure().isIndexed()).isTrue();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void should_parse_list() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @EmptyCollectionIfNull
            @Column(staticColumn = true)
            private List<String> friends;

            @NotNull
            @Column
            private List<String> mates;

            public List<String> getFriends() {
                return friends;
            }

            public void setFriends(List<String> friends) {
                this.friends = friends;
            }

            public List<String> getMates() {
                return mates;
            }

            public void setMates(List<String> mates) {
                this.mates = mates;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("friends"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getPropertyName()).isEqualTo("friends");
        assertThat(meta.<String>getValueClass()).isEqualTo(String.class);

        assertThat(meta.getGetter().getName()).isEqualTo("getFriends");
        assertThat((Class<List>) meta.getGetter().getReturnType()).isEqualTo(List.class);
        assertThat(meta.getSetter().getName()).isEqualTo("setFriends");
        assertThat((Class<List>) meta.getSetter().getParameterTypes()[0]).isEqualTo(List.class);

        assertThat(meta.type()).isEqualTo(PropertyType.LIST);
        assertThat(meta.forValues().nullValueForCollectionAndMap()).isNotNull().isInstanceOf(List.class);
        assertThat(meta.structure().isStaticColumn()).isTrue();

        PropertyParsingContext context2 = newContext(Test.class, Test.class.getDeclaredField("mates"));
        PropertyMeta meta2 = parser.parse(context2);
        assertThat(meta2.type()).isEqualTo(PropertyType.LIST);
        assertThat(meta2.forValues().nullValueForCollectionAndMap()).isNotNull().isInstanceOf(List.class);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void should_parse_set() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column(staticColumn = true)
            private Set<Long> followers;

            public Set<Long> getFollowers() {
                return followers;
            }

            public void setFollowers(Set<Long> followers) {
                this.followers = followers;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("followers"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getPropertyName()).isEqualTo("followers");
        assertThat(meta.<Long>getValueClass()).isEqualTo(Long.class);

        assertThat(meta.getGetter().getName()).isEqualTo("getFollowers");
        assertThat((Class<Set>) meta.getGetter().getReturnType()).isEqualTo(Set.class);
        assertThat(meta.getSetter().getName()).isEqualTo("setFollowers");
        assertThat((Class<Set>) meta.getSetter().getParameterTypes()[0]).isEqualTo(Set.class);

        assertThat(meta.type()).isEqualTo(PropertyType.SET);
        assertThat(meta.structure().isStaticColumn()).isTrue();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void should_parse_map() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column(staticColumn = true)
            private Map<Integer, String> preferences;

            public Map<Integer, String> getPreferences() {
                return preferences;
            }

            public void setPreferences(Map<Integer, String> preferences) {
                this.preferences = preferences;
            }
        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("preferences"));
        PropertyMeta meta = parser.parse(context);

        assertThat(meta.getPropertyName()).isEqualTo("preferences");
        assertThat(meta.<String>getValueClass()).isEqualTo(String.class);
        assertThat(meta.type()).isEqualTo(PropertyType.MAP);

        assertThat(meta.<Integer>getKeyClass()).isEqualTo(Integer.class);

        assertThat(meta.getGetter().getName()).isEqualTo("getPreferences");
        assertThat((Class<Map>) meta.getGetter().getReturnType()).isEqualTo(Map.class);
        assertThat(meta.getSetter().getName()).isEqualTo("setPreferences");
        assertThat((Class<Map>) meta.getSetter().getParameterTypes()[0]).isEqualTo(Map.class);
        assertThat(meta.structure().isStaticColumn()).isTrue();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void should_exception_for_nested_collection() throws Exception {
        @SuppressWarnings("unused")
        @Entity(keyspace = "ks", table="test")
        class Test {
            @Column
            private Map<Integer, List<String>> map;

            public Map<Integer, List<String>> getMap() {
                return map;
            }

            public void setMap(Map<Integer, List<String>> map) {
                this.map = map;
            }

        }
        PropertyParsingContext context = newContext(Test.class, Test.class.getDeclaredField("map"));
        expectedEx.expect(AchillesBeanMappingException.class);
        expectedEx.expectMessage("The type 'java.util.List' on field 'map' of entity 'null' is not supported. If you want to convert it to JSON string, do not forget to add @JSON");
        parser.parse(context);
    }

    @Test
    public void should_find_index() throws Exception {

        @Entity(keyspace = "ks", table="test")
        class Test {
            @Index
            private String name;
        }

        Field field = Test.class.getDeclaredField("name");

        assertThat(PropertyParser.getIndexName(field) != null).isTrue();
    }

    @Test
    public void should_not_find_counter_if_not_long_type() throws Exception {

    }

    @Test
    public void should_return_true_when_type_supported() throws Exception {
        assertThat(PropertyParser.isSupportedNativeType(Long.class)).isTrue();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void should_infer_entity_class_from_interceptor() throws Exception {
        assertThat(parser.inferEntityClassFromInterceptor(longInterceptor)).isEqualTo((Class) Long.class);
    }

    @Test
    public void should_exception_when_no_default_constructor_for_embeddedid() throws Exception {
        //Given
        @Entity(keyspace = "ks", table="test")
         class TestWithEmbeddedId {
            @EmbeddedId
            private Embedded id;


             public Embedded getId() {
                 return id;
             }

             public void setId(Embedded id) {
                 this.id = id;
             }

             class Embedded {

                public Embedded(String text) {

                }
            }

        }
        PropertyParsingContext context = newContext(TestWithEmbeddedId.class, TestWithEmbeddedId.class.getDeclaredField("id"));
        context.setEmbeddedId(true);

        expectedEx.expect(AchillesException.class);
        expectedEx.expectMessage("Cannot instantiate class of type null, did you forget to declare a default constructor ?");


        //When
        parser.parse(context);

        //Then

    }

    private Interceptor<Long> longInterceptor = new Interceptor<Long>() {
        @Override
        public void onEvent(Long entity) {

        }

        @Override
        public List<Event> events() {
            return null;
        }
    };

    private <T> PropertyParsingContext newContext(Class<T> entityClass, Field field) {
        entityContext = new EntityParsingContext(configContext, entityClass);

        return entityContext.newPropertyContext(field);
    }

}
