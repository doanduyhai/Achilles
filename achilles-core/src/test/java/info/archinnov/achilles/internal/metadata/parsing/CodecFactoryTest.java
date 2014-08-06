package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.annotations.Enumerated.Encoding.NAME;
import static info.archinnov.achilles.annotations.Enumerated.Encoding.ORDINAL;
import static org.fest.assertions.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.metadata.transcoding.codec.*;
import info.archinnov.achilles.test.builders.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class CodecFactoryTest {
    
    @Mock
    private EntityParsingContext context;

    private CodecFactory parser = new CodecFactory();

    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp() {
        Mockito.when(context.getCurrentObjectMapper()).thenReturn(mapper);
    }

    @Test
    public void should_create_native_codec() throws Exception {
        //Given
        class Test {
            private String name;
        }

        Field field = Test.class.getDeclaredField("name");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(NativeCodec.class);
        assertThat(codec.encode("toto")).isEqualTo("toto");
        assertThat(codec.decode("toto")).isEqualTo("toto");
    }

    @Test
    public void should_create_enum_name_codec() throws Exception {
        //Given
        class Test {
            private PropertyType type;
        }

        Field field = Test.class.getDeclaredField("type");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(EnumNameCodec.class);
        assertThat(codec.encode(PropertyType.COUNTER)).isEqualTo("COUNTER");
        assertThat(codec.decode("ID")).isEqualTo(PropertyType.ID);
    }

    @Test
    public void should_create_enum_ordinal_codec() throws Exception {
        //Given
        class Test {
            @Enumerated(ORDINAL)
            private PropertyType type;
        }

        Field field = Test.class.getDeclaredField("type");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(EnumOrdinalCodec.class);
        assertThat(codec.encode(PropertyType.COUNTER)).isEqualTo(6);
        assertThat(codec.decode(0)).isEqualTo(PropertyType.ID);
    }

    @Test
    public void should_create_byte_primitive_codec() throws Exception {
        //Given
        class Test {
            private byte flag;
        }

        Field field = Test.class.getDeclaredField("flag");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(ByteCodec.class);
        assertThat(((ByteBuffer)codec.encode((byte)3)).array()).containsOnly((byte) 3);
        assertThat(codec.decode(ByteBuffer.wrap(new byte[]{(byte)5}))).isEqualTo((byte)5);
    }

    @Test
    public void should_create_byte_object_codec() throws Exception {
        //Given
        class Test {
            private Byte flag;
        }

        Field field = Test.class.getDeclaredField("flag");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(ByteCodec.class);
        assertThat(((ByteBuffer)codec.encode(new Byte((byte)3))).array()).containsOnly((byte) 3);
        assertThat(codec.decode(ByteBuffer.wrap(new byte[]{(byte)5}))).isEqualTo((byte)5);
    }


    @Test
    public void should_create_byte_primitive_array_codec() throws Exception {
        //Given
        class Test {
            private byte[] bytes;
        }

        Field field = Test.class.getDeclaredField("bytes");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(ByteArrayPrimitiveCodec.class);
        assertThat(((ByteBuffer)codec.encode(new byte[]{(byte)2,(byte)3})).array()).containsOnly((byte) 2, (byte) 3);
        assertThat((byte[])codec.decode(ByteBuffer.wrap(new byte[]{(byte)5, (byte)6}))).containsOnly((byte) 5, (byte) 6);
    }

    @Test
    public void should_create_byte_object_array_codec() throws Exception {
        //Given
        class Test {
            private Byte[] bytes;
        }

        Field field = Test.class.getDeclaredField("bytes");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(ByteArrayCodec.class);
        assertThat(((ByteBuffer)codec.encode(new Byte[]{(byte)2,(byte)3})).array()).containsOnly((byte) 2, (byte) 3);
        assertThat((Byte[]) codec.decode(ByteBuffer.wrap(new byte[]{(byte)5, (byte)6}))).containsOnly((byte) 5, (byte) 6);
    }

    @Test
    public void should_create_JSON_codec() throws Exception {
        class Test {
            private Pojo json;
        }

        Pojo bean = new Pojo(10L, "DuyHai");

        Field field = Test.class.getDeclaredField("json");

        //When
        final SimpleCodec codec = parser.parseSimpleField(createContext(field));
        final String encoded = (String) codec.encode(bean);
        final Pojo decoded = (Pojo) codec.decode("{\"id\":11,\"name\":\"John\"}");

        //Then
        assertThat(encoded).isEqualTo("{\"id\":10,\"name\":\"DuyHai\"}");
        assertThat(decoded.getId()).isEqualTo(11L);
        assertThat(decoded.getName()).isEqualTo("John");
    }


    @Test
    public void should_create_list_codec() throws Exception {
        class Test {
            private List<Integer> counts;
        }

        Field field = Test.class.getDeclaredField("counts");

        //When
        final ListCodec<Object, Object> codec = parser.parseListField(createContext(field));
        final List<Object> encoded = codec.encode(Arrays.<Object>asList(1, 2, 3));
        final List<Object> decoded = codec.decode(Arrays.<Object>asList(4, 5));

        //Then
        assertThat(encoded).containsExactly(1, 2, 3);
        assertThat(decoded).containsExactly(4, 5);
    }

    @Test
    public void should_create_list_enum_codec() throws Exception {
        class Test {
            @Enumerated(ORDINAL)
            private List<PropertyType> types;
        }

        Field field = Test.class.getDeclaredField("types");

        //When
        final ListCodec codec = parser.parseListField(createContext(field));
        final List<Object> encoded = codec.encode(Arrays.<Object>asList(PropertyType.ID, PropertyType.EMBEDDED_ID));
        final List<Object> decoded = codec.decode(Arrays.<Object>asList(2, 3, 4));

        //Then
        assertThat(encoded).containsExactly(0, 1);
        assertThat(decoded).containsExactly(PropertyType.SIMPLE, PropertyType.LIST, PropertyType.SET);
    }

    @Test
    public void should_create_set_codec() throws Exception {
        class Test {
            private Set<Integer> counts;
        }

        Field field = Test.class.getDeclaredField("counts");

        //When
        final SetCodec<Object, Object> codec = parser.parseSetField(createContext(field));
        final Set<Object> encoded = codec.encode(Sets.<Object>newSet(1, 2, 3));
        final Set<Object> decoded = codec.decode(Sets.<Object>newSet(4, 5));

        //Then
        assertThat(encoded).containsOnly(1, 2, 3);
        assertThat(decoded).containsOnly(4, 5);
    }

    @Test
    public void should_create_set_enum_codec() throws Exception {
        class Test {
            @Enumerated(ORDINAL)
            private Set<PropertyType> types;
        }

        Field field = Test.class.getDeclaredField("types");

        //When
        final SetCodec codec = parser.parseSetField(createContext(field));
        final Set<Object> encoded = codec.encode(Sets.newSet(PropertyType.ID, PropertyType.EMBEDDED_ID));
        final Set<Object> decoded = codec.decode(Sets.newSet(2, 3, 4));

        //Then
        assertThat(encoded).containsOnly(0, 1);
        assertThat(decoded).containsOnly(PropertyType.SIMPLE, PropertyType.LIST, PropertyType.SET);
    }

    @Test
    public void should_create_map_codec() throws Exception {
        class Test {
            private Map<PropertyType, ElementType> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec codec = parser.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(PropertyType.COUNTER, ElementType.FIELD, PropertyType.ID, ElementType.METHOD));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of("LIST", "CONSTRUCTOR", "SET", "PARAMETER"));

        //Then
        assertThat(encoded.get("COUNTER")).isEqualTo("FIELD");
        assertThat(encoded.get("ID")).isEqualTo("METHOD");

        assertThat(decoded.get(PropertyType.LIST)).isEqualTo(ElementType.CONSTRUCTOR);
        assertThat(decoded.get(PropertyType.SET)).isEqualTo(ElementType.PARAMETER);
    }

    @Test
    public void should_create_enum_map_codec() throws Exception {
        class Test {
            @Enumerated(key = ORDINAL, value = NAME)
            private Map<PropertyType, ElementType> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec codec = parser.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(PropertyType.ID, ElementType.FIELD, PropertyType.EMBEDDED_ID, ElementType.METHOD));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of(3, "CONSTRUCTOR", 4, "PARAMETER"));

        //Then
        assertThat(encoded.get(0)).isEqualTo("FIELD");
        assertThat(encoded.get(1)).isEqualTo("METHOD");

        assertThat(decoded.get(PropertyType.LIST)).isEqualTo(ElementType.CONSTRUCTOR);
        assertThat(decoded.get(PropertyType.SET)).isEqualTo(ElementType.PARAMETER);
    }

    @Test
    public void should_create_key_map_codec() throws Exception {
        //Given
        class Test {
            private Map<PropertyType, Integer> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec<Object, Object, Object, Object> codec = parser.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(PropertyType.COUNTER, 1, PropertyType.ID, 2));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of("LIST", 3, "SET", 4));

        //Then
        assertThat(encoded.get("COUNTER")).isEqualTo(1);
        assertThat(encoded.get("ID")).isEqualTo(2);

        assertThat(decoded.get(PropertyType.LIST)).isEqualTo(3);
        assertThat(decoded.get(PropertyType.SET)).isEqualTo(4);
    }

    @Test
    public void should_create_key_enum_map_codec() throws Exception {
        //Given
        class Test {
            @Enumerated(key = ORDINAL)
            private Map<PropertyType, Integer> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec<Object, Object, Object, Object> codec = parser.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(PropertyType.ID, 100, PropertyType.EMBEDDED_ID, 200));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of(3, 3, 4, 4));

        //Then
        assertThat(encoded.get(0)).isEqualTo(100);
        assertThat(encoded.get(1)).isEqualTo(200);

        assertThat(decoded.get(PropertyType.LIST)).isEqualTo(3);
        assertThat(decoded.get(PropertyType.SET)).isEqualTo(4);
    }

    @Test
    public void should_create_value_map_codec() throws Exception {
        class Test {
            private Map<Integer,PropertyType> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec codec = parser.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(1, PropertyType.COUNTER, 2, PropertyType.ID));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of(3, "LIST", 4, "SET"));

        //Then
        assertThat(encoded.get(1)).isEqualTo("COUNTER");
        assertThat(encoded.get(2)).isEqualTo("ID");

        assertThat(decoded.get(3)).isEqualTo(PropertyType.LIST);
        assertThat(decoded.get(4)).isEqualTo(PropertyType.SET);
    }

    @Test
    public void should_create_value_enum_map_codec() throws Exception {
        class Test {
            @Enumerated(ORDINAL)
            private Map<Integer,PropertyType> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec codec = parser.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(1, PropertyType.ID, 2, PropertyType.EMBEDDED_ID));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of(3, 3, 4, 4));

        //Then
        assertThat(encoded.get(1)).isEqualTo(0);
        assertThat(encoded.get(2)).isEqualTo(1);

        assertThat(decoded.get(3)).isEqualTo(PropertyType.LIST);
        assertThat(decoded.get(4)).isEqualTo(PropertyType.SET);
    }

    private PropertyParsingContext createContext(Field field) {
        return new PropertyParsingContext(context, field);
    }

    public static class Pojo {
        private Long id;
        private String name;

        public Pojo() {
        }

        public Pojo(Long id, String name) {
            this.id = id;
            this.name = name;
        }

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
}