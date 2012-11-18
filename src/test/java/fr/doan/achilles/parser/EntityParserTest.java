package fr.doan.achilles.parser;

import static fr.doan.achilles.metadata.PropertyType.SIMPLE;
import static fr.doan.achilles.serializer.Utils.LONG_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static org.fest.assertions.Assertions.assertThat;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import me.prettyprint.hector.api.Keyspace;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import fr.doan.achilles.exception.IncorrectTypeException;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.PropertyType;
import fr.doan.achilles.metadata.SetPropertyMeta;
import fr.doan.achilles.serializer.Utils;

@RunWith(MockitoJUnitRunner.class)
public class EntityParserTest {
    private final EntityParser parser = new EntityParser();

    @Mock
    private Keyspace keyspace;

    @SuppressWarnings("unchecked")
    @Test
    public void should_parse_entity() throws Exception {
        EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, TestEntity.class);

        assertThat(meta.getCanonicalClassName()).isEqualTo("fr.doan.achilles.parser.EntityParserTest.TestEntity");
        assertThat(meta.getColumnFamilyName()).isEqualTo("fr_doan_achilles_parser_EntityParserTest_TestEntity");
        assertThat(meta.getSerialVersionUID()).isEqualTo(1L);
        assertThat(meta.getIdMeta().getValueClass()).isEqualTo(Long.class);
        assertThat(meta.getIdMeta().getPropertyName()).isEqualTo("id");
        assertThat(meta.getIdMeta().getValueSerializer().getComparatorType()).isEqualTo(LONG_SRZ.getComparatorType());
        assertThat(meta.getIdSerializer()).isEqualTo(LONG_SRZ);
        assertThat(meta.getPropertyMetas()).hasSize(5);

        PropertyMeta<?> name = meta.getPropertyMetas().get("name");
        PropertyMeta<?> age = meta.getPropertyMetas().get("age_in_year");
        ListPropertyMeta<String> friends = (ListPropertyMeta<String>) meta.getPropertyMetas().get("friends");
        SetPropertyMeta<String> followers = (SetPropertyMeta<String>) meta.getPropertyMetas().get("followers");
        MapPropertyMeta<String> preferences = (MapPropertyMeta<String>) meta.getPropertyMetas().get("preferences");

        assertThat(name).isNotNull();
        assertThat(age).isNotNull();
        assertThat(friends).isNotNull();
        assertThat(followers).isNotNull();
        assertThat(preferences).isNotNull();

        assertThat(name.getPropertyName()).isEqualTo("name");
        assertThat(name.getValueClass()).isEqualTo(String.class);
        assertThat(name.getValueSerializer()).isEqualTo(STRING_SRZ);
        assertThat(name.propertyType()).isEqualTo(SIMPLE);

        assertThat(age.getPropertyName()).isEqualTo("age_in_year");
        assertThat(age.getValueClass()).isEqualTo(Long.class);
        assertThat(age.getValueSerializer()).isEqualTo(LONG_SRZ);
        assertThat(age.propertyType()).isEqualTo(SIMPLE);

        assertThat(friends.getPropertyName()).isEqualTo("friends");
        assertThat(friends.getValueClass()).isEqualTo(String.class);
        assertThat(friends.getValueSerializer()).isEqualTo(STRING_SRZ);
        assertThat(friends.propertyType()).isEqualTo(PropertyType.LIST);
        assertThat(friends.newListInstance()).isNotNull();
        assertThat(friends.newListInstance()).isEmpty();
        assertThat(friends.newListInstance().getClass()).isEqualTo(ArrayList.class);

        assertThat(followers.getPropertyName()).isEqualTo("followers");
        assertThat(followers.getValueClass()).isEqualTo(String.class);
        assertThat(followers.getValueSerializer()).isEqualTo(STRING_SRZ);
        assertThat(followers.propertyType()).isEqualTo(PropertyType.SET);
        assertThat(followers.newSetInstance()).isNotNull();
        assertThat(followers.newSetInstance()).isEmpty();
        assertThat(followers.newSetInstance().getClass()).isEqualTo(HashSet.class);

        assertThat(preferences.getPropertyName()).isEqualTo("preferences");
        assertThat(preferences.getValueClass()).isEqualTo(String.class);
        assertThat(preferences.getValueSerializer()).isEqualTo(STRING_SRZ);
        assertThat(preferences.propertyType()).isEqualTo(PropertyType.MAP);
        assertThat(preferences.getKeyClass()).isEqualTo(Integer.class);
        assertThat(preferences.getKeySerializer()).isEqualTo(Utils.INT_SRZ);
        assertThat(preferences.newMapInstance()).isNotNull();
        assertThat(preferences.newMapInstance()).isEmpty();
        assertThat(preferences.newMapInstance().getClass()).isEqualTo(HashMap.class);
    }

    @SuppressWarnings({ "unused", "unchecked" })
    @Test
    public void should_parse_entity_with_table_name() throws Exception {
        @Table(name = "myOwnCF")
        class TestEntityWithColumnFamilyName implements Serializable {
            public static final long serialVersionUID = 1234L;

            @Id
            private Long id;

            @Column
            private String name;

            public Long getId() {
                return id;
            }

            public void setId(Long id) {
                this.id = id;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

        }

        EntityMeta<Long> meta = (EntityMeta<Long>) parser.parseEntity(keyspace, TestEntityWithColumnFamilyName.class);

        assertThat(meta).isNotNull();
        assertThat(meta.getColumnFamilyName()).isEqualTo("myOwnCF");
    }

    @Test(expected = IncorrectTypeException.class)
    public void should_exception_when_entity_has_no_table_annotation() throws Exception {
        @SuppressWarnings("serial")
        @Table
        class TestEntityNoTableAnnotation implements Serializable {
        }
        parser.parseEntity(keyspace, TestEntityNoTableAnnotation.class);
    }

    @Test(expected = IncorrectTypeException.class)
    public void should_exception_when_entity_has_no_serialVersionUID() throws Exception {
        @SuppressWarnings("serial")
        @Table
        class TestEntityNoSerialVersionUID implements Serializable {
        }
        parser.parseEntity(keyspace, TestEntityNoSerialVersionUID.class);
    }

    @Test(expected = IncorrectTypeException.class)
    public void should_exception_when_entity_has_non_public_serialVersionUID() throws Exception {
        @Table
        class TestEntityNonPublicSerialVersionUID implements Serializable {
            static final long serialVersionUID = 1L;
        }
        parser.parseEntity(keyspace, TestEntityNonPublicSerialVersionUID.class);
    }

    @Test(expected = IncorrectTypeException.class)
    public void should_exception_when_entity_has_no_id() throws Exception {
        @Table
        class TestEntityNoId implements Serializable {
            public static final long serialVersionUID = 1L;
        }
        parser.parseEntity(keyspace, TestEntityNoId.class);
    }

    @Test(expected = ValidationException.class)
    public void should_exception_when_id_type_not_serializable() throws Exception {
        class NotSerializableId {
        }
        @Table
        class TestEntityNotSerializableId implements Serializable {
            public static final long serialVersionUID = 1L;

            @Id
            private NotSerializableId id;
        }

        parser.parseEntity(keyspace, TestEntityNotSerializableId.class);
    }

    @SuppressWarnings("unused")
    @Test(expected = IncorrectTypeException.class)
    public void should_exception_when_entity_has_no_column() throws Exception {
        @Table
        class TestEntityNoColumn implements Serializable {
            public static final long serialVersionUID = 1L;

            @Id
            private Long id;

            public Long getId() {
                return id;
            }

            public void setId(Long id) {
                this.id = id;
            }

        }
        parser.parseEntity(keyspace, TestEntityNoColumn.class);
    }

    @Table
    public class TestEntity implements Serializable {
        public static final long serialVersionUID = 1L;

        @Id
        private Long id;

        @Column
        private String name;

        @Column(name = "age_in_year")
        private Long age;

        @Column
        private List<String> friends;

        @Column
        private Set<String> followers;

        @Column
        private Map<Integer, String> preferences;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getAge() {
            return age;
        }

        public void setAge(Long age) {
            this.age = age;
        }

        public List<String> getFriends() {
            return friends;
        }

        public void setFriends(List<String> friends) {
            this.friends = friends;
        }

        public Set<String> getFollowers() {
            return followers;
        }

        public void setFollowers(Set<String> followers) {
            this.followers = followers;
        }

        public Map<Integer, String> getPreferences() {
            return preferences;
        }

        public void setPreferences(Map<Integer, String> preferences) {
            this.preferences = preferences;
        }

    }

}
