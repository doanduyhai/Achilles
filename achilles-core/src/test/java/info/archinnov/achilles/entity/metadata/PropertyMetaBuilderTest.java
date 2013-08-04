package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.metadata.transcoding.CompoundTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.ListTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.MapTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SetTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.test.parser.entity.Bean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.ConsistencyLevel;
import org.apache.cassandra.utils.Pair;
import java.lang.reflect.Method;
import java.util.ArrayList;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

/**
 * PropertyMetaBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaBuilderTest {
    Method[] accessors = new Method[2];

    private ObjectMapper mapper = new ObjectMapper();

    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        accessors[0] = Bean.class.getDeclaredMethod("getId");
        accessors[1] = Bean.class.getDeclaredMethod("setId", Long.class);
    }

    @Test
    public void should_build_simple() throws Exception {

        PropertyMeta<Void, String> built = PropertyMetaBuilder
                .factory()
                .type(SIMPLE)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .consistencyLevels(Pair.create(ONE, ALL))
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(SIMPLE);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getReadConsistencyLevel()).isEqualTo(ONE);
        assertThat(built.getWriteConsistencyLevel()).isEqualTo(ALL);
        assertThat(built.getTranscoder()).isInstanceOf(SimpleTranscoder.class);
    }

    @Test
    public void should_build_compound_id() throws Exception {

        EmbeddedIdProperties props = new EmbeddedIdProperties();
        props.setComponentClasses(new ArrayList<Class<?>>());
        props.setComponentGetters(new ArrayList<Method>());
        props.setComponentSetters(new ArrayList<Method>());

        PropertyMeta<Void, CompoundKey> built = PropertyMetaBuilder
                .factory()
                .type(EMBEDDED_ID)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .consistencyLevels(Pair.create(ONE, ALL))
                .embeddedIdProperties(props)
                .build(Void.class, CompoundKey.class);

        assertThat(built.type()).isEqualTo(EMBEDDED_ID);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueClass()).isEqualTo(CompoundKey.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.isEmbeddedId()).isTrue();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getReadConsistencyLevel()).isEqualTo(ONE);
        assertThat(built.getWriteConsistencyLevel()).isEqualTo(ALL);
        assertThat(built.getTranscoder()).isInstanceOf(CompoundTranscoder.class);
    }

    @Test
    public void should_build_simple_lazy() throws Exception {

        PropertyMeta<Void, String> built = PropertyMetaBuilder
                .factory()
                .type(LAZY_SIMPLE)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(LAZY_SIMPLE);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isTrue();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(SimpleTranscoder.class);
    }

    @Test
    public void should_build_simple_with_object_as_value() throws Exception {
        PropertyMeta<Void, Bean> built = PropertyMetaBuilder
                .factory()
                .type(SIMPLE)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Void.class, Bean.class);

        assertThat(built.type()).isEqualTo(SIMPLE);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        Bean bean = new Bean();
        assertThat(built.getValueFromString(writeString(bean))).isInstanceOf(Bean.class);
        assertThat(built.getValueClass()).isEqualTo(Bean.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(SimpleTranscoder.class);
    }

    @Test
    public void should_build_list() throws Exception {

        PropertyMeta<Void, String> built = PropertyMetaBuilder
                .factory()
                .type(LIST)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(LIST);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(ListTranscoder.class);
    }

    @Test
    public void should_build_list_lazy() throws Exception {

        PropertyMeta<Void, String> built = PropertyMetaBuilder
                .factory()
                .type(LAZY_LIST)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(LAZY_LIST);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isTrue();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(ListTranscoder.class);
    }

    @Test
    public void should_build_set() throws Exception {

        PropertyMeta<Void, String> built = PropertyMetaBuilder
                .factory()
                .type(SET)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(SET);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(SetTranscoder.class);
    }

    @Test
    public void should_build_set_lazy() throws Exception {

        PropertyMeta<Void, String> built = PropertyMetaBuilder
                .factory()
                .type(LAZY_SET)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(LAZY_SET);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isTrue();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(SetTranscoder.class);
    }

    @Test
    public void should_build_map() throws Exception {

        PropertyMeta<Integer, String> built = PropertyMetaBuilder
                .factory()
                .type(MAP)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Integer.class, String.class);

        assertThat(built.type()).isEqualTo(MAP);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getKey(12)).isInstanceOf(Integer.class);
        assertThat(built.getKeyClass()).isEqualTo(Integer.class);

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(MapTranscoder.class);
    }

    @Test
    public void should_build_map_with_object_as_key() throws Exception {
        PropertyMeta<Bean, String> built = PropertyMetaBuilder
                .factory()
                .type(MAP)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Bean.class, String.class);

        assertThat(built.type()).isEqualTo(MAP);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        Bean bean = new Bean();
        assertThat(built.getKey(bean)).isInstanceOf(Bean.class);
        assertThat(built.getKeyClass()).isEqualTo(Bean.class);

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(MapTranscoder.class);
    }

    @Test
    public void should_build_map_lazy() throws Exception {

        PropertyMeta<Integer, String> built = PropertyMetaBuilder
                .factory()
                .type(LAZY_MAP)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .build(Integer.class, String.class);

        assertThat(built.type()).isEqualTo(LAZY_MAP);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.getKey(12)).isInstanceOf(Integer.class);
        assertThat(built.getKeyClass()).isEqualTo(Integer.class);

        assertThat(built.getValueFromString("\"val\"")).isInstanceOf(String.class);
        assertThat(built.getValueClass()).isEqualTo(String.class);

        assertThat(built.type().isLazy()).isTrue();
        assertThat(built.isEmbeddedId()).isFalse();
        assertThat(built.type().isJoin()).isFalse();
        assertThat(built.getTranscoder()).isInstanceOf(MapTranscoder.class);
    }

    private String writeString(Object value) throws Exception {
        return mapper.writeValueAsString(value);
    }
}
