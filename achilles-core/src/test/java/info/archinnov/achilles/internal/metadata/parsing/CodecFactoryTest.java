package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.annotations.Enumerated.Encoding.NAME;
import static info.archinnov.achilles.annotations.Enumerated.Encoding.ORDINAL;
import static info.archinnov.achilles.internal.metadata.parsing.TypeTransformerParserTest.EnumToStringCodec;
import static info.archinnov.achilles.internal.metadata.parsing.TypeTransformerParserTest.LongToStringCodec;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.annotations.TypeTransformer;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.codec.ByteArrayCodec;
import info.archinnov.achilles.internal.metadata.codec.ByteArrayPrimitiveCodec;
import info.archinnov.achilles.internal.metadata.codec.ByteCodec;
import info.archinnov.achilles.internal.metadata.codec.EnumNameCodec;
import info.archinnov.achilles.internal.metadata.codec.EnumOrdinalCodec;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.NativeCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import info.archinnov.achilles.internal.metadata.holder.InternalTimeUUID;
import info.archinnov.achilles.internal.metadata.holder.PropertyType;
import info.archinnov.achilles.internal.metadata.parsing.context.EntityParsingContext;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.NamingStrategy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class CodecFactoryTest {
    
    @Mock
    private EntityParsingContext context;

    private CodecFactory factory = new CodecFactory();

    private ObjectMapper mapper = new ObjectMapper();


    @Before
    public void setUp() {
        when(context.getCurrentObjectMapper()).thenReturn(mapper);
        when(context.getNamingStrategy()).thenReturn(NamingStrategy.LOWER_CASE);

    }

    @Test
    public void should_create_native_codec() throws Exception {
        //Given
        class Test {
            private String name;
        }

        Field field = Test.class.getDeclaredField("name");

        //When
        final Codec codec = factory.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(NativeCodec.class);
        assertThat(codec.encode("toto")).isEqualTo("toto");
        assertThat(codec.decode("toto")).isEqualTo("toto");
    }

    @Test
    public void should_create_simple_codec_from_transformer() throws Exception {
        //Given
        class Test {
            @TypeTransformer(valueCodecClass = LongToStringCodec.class)
            private Long field;
        }

        Field field = Test.class.getDeclaredField("field");

        //When
        final Codec codec = factory.parseSimpleField(createContext(field));

        //Then
        assertThat(codec.sourceType()).isSameAs(Long.class);
        assertThat(codec.targetType()).isSameAs(String.class);
    }

    @Test
    public void should_create_enum_name_codec() throws Exception {
        //Given
        class Test {
            private PropertyType type;
        }

        Field field = Test.class.getDeclaredField("type");

        //When
        final Codec codec = factory.parseSimpleField(createContext(field));

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
        final Codec codec = factory.parseSimpleField(createContext(field));

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
        final Codec codec = factory.parseSimpleField(createContext(field));

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
        final Codec codec = factory.parseSimpleField(createContext(field));

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
        final Codec codec = factory.parseSimpleField(createContext(field));

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
        final Codec codec = factory.parseSimpleField(createContext(field));

        //Then
        assertThat(codec).isInstanceOf(ByteArrayCodec.class);
        assertThat(((ByteBuffer)codec.encode(new Byte[]{(byte)2,(byte)3})).array()).containsOnly((byte) 2, (byte) 3);
        assertThat((Byte[]) codec.decode(ByteBuffer.wrap(new byte[]{(byte)5, (byte)6}))).containsOnly((byte) 5, (byte) 6);
    }

    @Test
    public void should_create_JSON_codec() throws Exception {
        class Test {
            @JSON
            private Pojo json;
        }

        Pojo bean = new Pojo(10L, "DuyHai");

        Field field = Test.class.getDeclaredField("json");

        //When
        final Codec codec = factory.parseSimpleField(createContext(field));
        final String encoded = (String) codec.encode(bean);
        final Pojo decoded = (Pojo) codec.decode("{\"id\":11,\"name\":\"John\"}");

        //Then
        assertThat(encoded).isEqualTo("{\"id\":10,\"name\":\"DuyHai\"}");
        assertThat(decoded.getId()).isEqualTo(11L);
        assertThat(decoded.getName()).isEqualTo("John");
    }

    @Test(expected = AchillesBeanMappingException.class)
    public void should_exception_for_unsupported_types() throws Exception {
        //Given
        class Test {
            private Pojo json;
        }

        Field field = Test.class.getDeclaredField("json");

        when(context.getCurrentEntityClass()).thenReturn((Class)Test.class);

        //When
        factory.parseSimpleField(createContext(field));

        //Then

    }

    @Test
    public void should_create_list_codec() throws Exception {
        class Test {
            private List<Integer> counts;
        }

        Field field = Test.class.getDeclaredField("counts");

        //When
        final ListCodec<Object, Object> codec = factory.parseListField(createContext(field));
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
        final ListCodec codec = factory.parseListField(createContext(field));
        final List<Object> encoded = codec.encode(Arrays.<Object>asList(PropertyType.ID, PropertyType.EMBEDDED_ID));
        final List<Object> decoded = codec.decode(Arrays.<Object>asList(2, 3, 4));

        //Then
        assertThat(encoded).containsExactly(0, 1);
        assertThat(decoded).containsExactly(PropertyType.SIMPLE, PropertyType.LIST, PropertyType.SET);
    }

    @Test
    public void should_create_list_codec_from_transformer() throws Exception {
        class Test {
            @TypeTransformer(valueCodecClass = LongToStringCodec.class)
            private List<Long> counts;
        }

        Field field = Test.class.getDeclaredField("counts");
        //When
        final ListCodec codec = factory.parseListField(createContext(field));

        //Then
        assertThat(codec.sourceType()).isSameAs(Long.class);
        assertThat(codec.targetType()).isSameAs(String.class);
    }

    @Test
    public void should_create_set_codec() throws Exception {
        class Test {
            private Set<Integer> counts;
        }

        Field field = Test.class.getDeclaredField("counts");

        //When
        final SetCodec<Object, Object> codec = factory.parseSetField(createContext(field));
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
        final SetCodec codec = factory.parseSetField(createContext(field));
        final Set<Object> encoded = codec.encode(Sets.newSet(PropertyType.ID, PropertyType.EMBEDDED_ID));
        final Set<Object> decoded = codec.decode(Sets.newSet(2, 3, 4));

        //Then
        assertThat(encoded).containsOnly(0, 1);
        assertThat(decoded).containsOnly(PropertyType.SIMPLE, PropertyType.LIST, PropertyType.SET);
    }

    @Test
    public void should_create_set_codec_from_transformer() throws Exception {
        class Test {
            @TypeTransformer(valueCodecClass = LongToStringCodec.class)
            private Set<Long> counts;
        }

        Field field = Test.class.getDeclaredField("counts");
        //When
        final SetCodec codec = factory.parseSetField(createContext(field));

        //Then
        assertThat(codec.sourceType()).isSameAs(Long.class);
        assertThat(codec.targetType()).isSameAs(String.class);
    }

    @Test
    public void should_create_map_codec() throws Exception {
        class Test {
            private Map<PropertyType, ElementType> maps;
        }

        Field field = Test.class.getDeclaredField("maps");

        //When
        final MapCodec codec = factory.parseMapField(createContext(field));
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
        final MapCodec codec = factory.parseMapField(createContext(field));
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
        final MapCodec<Object, Object, Object, Object> codec = factory.parseMapField(createContext(field));
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
        final MapCodec<Object, Object, Object, Object> codec = factory.parseMapField(createContext(field));
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
        final MapCodec codec = factory.parseMapField(createContext(field));
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
        final MapCodec codec = factory.parseMapField(createContext(field));
        Map<Object, Object> encoded = codec.encode(ImmutableMap.<Object, Object>of(1, PropertyType.ID, 2, PropertyType.EMBEDDED_ID));
        Map<Object, Object> decoded = codec.decode(ImmutableMap.<Object, Object>of(3, 3, 4, 4));

        //Then
        assertThat(encoded.get(1)).isEqualTo(0);
        assertThat(encoded.get(2)).isEqualTo(1);

        assertThat(decoded.get(3)).isEqualTo(PropertyType.LIST);
        assertThat(decoded.get(4)).isEqualTo(PropertyType.SET);
    }

    @Test
    public void should_create_map_codec_from_transformer() throws Exception {
        class Test {
            @TypeTransformer(keyCodecClass = LongToStringCodec.class, valueCodecClass = EnumToStringCodec.class)
            private Map<Long,NamingStrategy> map;
        }

        Field field = Test.class.getDeclaredField("map");
        //When
        final MapCodec codec = factory.parseMapField(createContext(field));

        //Then
        assertThat(codec.sourceKeyType()).isSameAs(Long.class);
        assertThat(codec.targetKeyType()).isSameAs(String.class);

        assertThat(codec.sourceValueType()).isSameAs(NamingStrategy.class);
        assertThat(codec.targetValueType()).isSameAs(String.class);
    }

    @Test
    public void should_determine_cql3_simple_type() throws Exception {
        //Given
        Codec simpleCodec = new NativeCodec(String.class);

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(simpleCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)String.class);
    }

    @Test
    public void should_determine_cql3_simple_timeuuid_type() throws Exception {
        //Given
        Codec simpleCodec = new NativeCodec(UUID.class);

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(simpleCodec, true);

        //Then
        assertThat(actualClass).isEqualTo((Class)InternalTimeUUID.class);
    }

    @Test
    public void should_determine_cql3_simple_byte_buffer_type() throws Exception {
        //Given
        Codec simpleCodec = new NativeCodec(ByteBuffer.wrap("test".getBytes()).getClass());

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(simpleCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)ByteBuffer.class);
    }

    @Test
    public void should_determine_cql3_simple_counter_type() throws Exception {
        //Given
        Codec simpleCodec = new NativeCodec(Counter.class);

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(simpleCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)Long.class);
    }


    @Test
    public void should_determine_cql3_list_type() throws Exception {
        //Given
        ListCodec listCodec = mock(ListCodec.class);
        when(listCodec.sourceType()).thenReturn(Integer.class);
        when(listCodec.targetType()).thenReturn(String.class);

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(listCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)String.class);
    }

    @Test
    public void should_determine_cql3_set_type() throws Exception {
        //Given
        SetCodec setCodec = mock(SetCodec.class);
        when(setCodec.sourceType()).thenReturn(Integer.class);
        when(setCodec.targetType()).thenReturn(String.class);

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(setCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)String.class);
    }

    @Test
    public void should_determine_cql3_map_value_type() throws Exception {
        //Given
        MapCodec mapCodec = mock(MapCodec.class);
        when(mapCodec.sourceValueType()).thenReturn(Integer.class);
        when(mapCodec.targetValueType()).thenReturn(String.class);

        //When
        final Class<?> actualClass = factory.determineCQL3ValueType(mapCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)String.class);
    }

    @Test
    public void should_determine_cql3_map_key_type() throws Exception {
        //Given
        MapCodec mapCodec = mock(MapCodec.class);
        when(mapCodec.sourceKeyType()).thenReturn(Integer.class);
        when(mapCodec.targetKeyType()).thenReturn(String.class);

        //When
        final Class<?> actualClass = factory.determineCQL3KeyType(mapCodec, false);

        //Then
        assertThat(actualClass).isEqualTo((Class)String.class);
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