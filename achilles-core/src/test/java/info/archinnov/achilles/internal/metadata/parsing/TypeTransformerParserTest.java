package info.archinnov.achilles.internal.metadata.parsing;

import static java.lang.String.format;
import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.annotations.TypeTransformer;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.codec.IdentityCodec;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import info.archinnov.achilles.type.NamingStrategy;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class TypeTransformerParserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TypeTransformerParser parser = new TypeTransformerParser();

    @Test
    public void should_parse_simple_type_transformer() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = LongToStringCodec.class)
            private Long longToString;
        }

        //When
        final Codec actual = parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));

        //Then
        assertThat(actual).isInstanceOf(LongToStringCodec.class);
        assertThat(actual.sourceType()).isSameAs(Long.class);
        assertThat(actual.targetType()).isSameAs(String.class);
    }

    @Test
    public void should_exception_when_codec_class_not_instance_of_codec() throws Exception {
        //Given
        class MyCodec {}

        class Test {

            @TypeTransformer(valueCodecClass = MyCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The codec class '%s' declared in @TypeTransformer on the field '%s' of class '%s' should implement the interface Codec<FROM,TO>",
                MyCodec.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_codec_class_is_identity() throws Exception {
        //Given
        class MyCodec {}

        class Test {

            @TypeTransformer(valueCodecClass = IdentityCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The @TypeTransformer on the field '%s' of class '%s' should declare a value codec other than IdentityCodec. Maybe you forgot to provided it ?",
                "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_codec_class_not_provided() throws Exception {
        //Given
        class MyCodec {}

        class Test {

            @TypeTransformer(valueCodecClass = IdentityCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The @TypeTransformer on the field '%s' of class '%s' should declare a value codec other than IdentityCodec. Maybe you forgot to provided it ?",
                "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_codec_not_instantiable() throws Exception {
        //Given
        class MyCodec implements Codec<Integer, String> {

            private MyCodec() {}

            @Override
            public Class<Integer> sourceType() {
                return Integer.class;
            }

            @Override
            public Class<String> targetType() {
                return String.class;
            }

            @Override
            public String encode(Integer fromJava) throws AchillesTranscodingException {
                return null;
            }

            @Override
            public Integer decode(String fromCassandra) throws AchillesTranscodingException {
                return null;
            }
        };

        class Test {

            @TypeTransformer(valueCodecClass = MyCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Codec class '%s' declared on the field '%s' of class '%s' should be instantiable (declare a public constructor)",
                MyCodec.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_source_type_null() throws Exception {
        //Given

        class Test {

            @TypeTransformer(valueCodecClass = NullSourceTypeCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Source type of codec declared in annotation @TypeTransformer on the field '%s' of class '%s' should not be null",
                "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_target_type_null() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NullTargetTypeCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Target type of codec declared in annotation @TypeTransformer on the field '%s' of class '%s' should not be null",
                "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_source_type_not_match_for_simple_codec() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NonMatchingSourceTypeCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Source type '%s' of codec declared in annotation @TypeTransformer does not match Java type '%s' found on the field '%s' of class '%s'",
                Integer.class.getCanonicalName(), Long.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_target_type_not_supported_for_simple_codec() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NonMatchingTargetTypeCodec.class)
            private Long longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Target type '%s' declared on the field '%s' of class '%s' is not supported as primitive Cassandra data type",
                StringUtils.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateSimpleCodec(Test.class.getDeclaredField("longToString"));
    }


    @Test
    public void should_parse_list_type_transformer() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = LongToStringCodec.class)
            private List<Long> longToString;
        }

        //When
        final ListCodec actual = parser.parseAndValidateListCodec(Test.class.getDeclaredField("longToString"));

        //Then
        assertThat(actual.sourceType()).isSameAs(Long.class);
        assertThat(actual.targetType()).isSameAs(String.class);
    }

    @Test
    public void should_exception_when_source_type_not_match_for_list_codec() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NonMatchingSourceTypeCodec.class)
            private List<Long> longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Source type '%s' of codec declared in annotation @TypeTransformer does not match Java type '%s' found on the field '%s' of class '%s'",
                Integer.class.getCanonicalName(), Long.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateListCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_target_type_not_supported_for_list_codec() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NonMatchingTargetTypeCodec.class)
            private List<Long> longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Target type '%s' declared on the field '%s' of class '%s' is not supported as primitive Cassandra data type",
                StringUtils.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateListCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_parse_set_type_transformer() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = LongToStringCodec.class)
            private Set<Long> longToString;
        }

        //When
        final SetCodec actual = parser.parseAndValidateSetCodec(Test.class.getDeclaredField("longToString"));

        //Then
        assertThat(actual.sourceType()).isSameAs(Long.class);
        assertThat(actual.targetType()).isSameAs(String.class);
    }

    @Test
    public void should_exception_when_source_type_not_match_for_set_codec() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NonMatchingSourceTypeCodec.class)
            private Set<Long> longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Source type '%s' of codec declared in annotation @TypeTransformer does not match Java type '%s' found on the field '%s' of class '%s'",
                Integer.class.getCanonicalName(), Long.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateSetCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_target_type_not_supported_for_set_codec() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = NonMatchingTargetTypeCodec.class)
            private Set<Long> longToString;
        }

        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("Target type '%s' declared on the field '%s' of class '%s' is not supported as primitive Cassandra data type",
                StringUtils.class.getCanonicalName(), "longToString", "null"));

        parser.parseAndValidateSetCodec(Test.class.getDeclaredField("longToString"));
    }

    @Test
    public void should_exception_when_key_and_value_codec_not_set_on_map() throws Exception {
        //Given
        class Test {

            @TypeTransformer
            private Map<Integer,Long> map;
        }
        exception.expect(AchillesBeanMappingException.class);
        exception.expectMessage(format("The @TypeTransformer on the field '%s' of class '%s' should declare a key/value codec other than IdentityCodec. Maybe you forgot to provided it ?",
                "map", "null"));

        parser.parseAndValidateMapCodec(Test.class.getDeclaredField("map"));
    }


    @Test
    public void should_parse_map_key_and_value_type_transformer() throws Exception {
        //Given
        class Test {

            @TypeTransformer(keyCodecClass = LongToStringCodec.class, valueCodecClass = EnumToStringCodec.class)
            private Map<Long,NamingStrategy> map;
        }

        //When
        final MapCodec actual = parser.parseAndValidateMapCodec(Test.class.getDeclaredField("map"));

        //Then
        assertThat(actual.sourceKeyType()).isSameAs(Long.class);
        assertThat(actual.targetKeyType()).isSameAs(String.class);

        assertThat(actual.sourceValueType()).isSameAs(NamingStrategy.class);
        assertThat(actual.targetValueType()).isSameAs(String.class);
    }

    @Test
    public void should_parse_map_key_type_transformer() throws Exception {
        //Given
        class Test {

            @TypeTransformer(keyCodecClass = LongToStringCodec.class)
            private Map<Long,String> map;
        }

        //When
        final MapCodec actual = parser.parseAndValidateMapCodec(Test.class.getDeclaredField("map"));

        //Then
        assertThat(actual.sourceKeyType()).isSameAs(Long.class);
        assertThat(actual.targetKeyType()).isSameAs(String.class);

        assertThat(actual.sourceValueType()).isSameAs(String.class);
        assertThat(actual.targetValueType()).isSameAs(String.class);
    }

    @Test
    public void should_parse_map_value_type_transformer() throws Exception {
        //Given
        class Test {

            @TypeTransformer(valueCodecClass = EnumToStringCodec.class)
            private Map<Long,NamingStrategy> map;
        }

        //When
        final MapCodec actual = parser.parseAndValidateMapCodec(Test.class.getDeclaredField("map"));

        //Then
        assertThat(actual.sourceKeyType()).isSameAs(Long.class);
        assertThat(actual.targetKeyType()).isSameAs(Long.class);

        assertThat(actual.sourceValueType()).isSameAs(NamingStrategy.class);
        assertThat(actual.targetValueType()).isSameAs(String.class);
    }

    public static class LongToStringCodec implements Codec<Long, String> {

        @Override
        public Class<Long> sourceType() {
            return Long.class;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public String encode(Long fromJava) throws AchillesTranscodingException {
            return fromJava.toString();
        }

        @Override
        public Long decode(String fromCassandra) throws AchillesTranscodingException {
            return Long.parseLong(fromCassandra);
        }
    }

    public static class EnumToStringCodec implements Codec<NamingStrategy, String> {

        @Override
        public Class<NamingStrategy> sourceType() {
            return NamingStrategy.class;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public String encode(NamingStrategy fromJava) throws AchillesTranscodingException {
            return fromJava.name();
        }

        @Override
        public NamingStrategy decode(String fromCassandra) throws AchillesTranscodingException {
            return NamingStrategy.valueOf(fromCassandra);
        }

    }

    public static class NullSourceTypeCodec implements Codec<Integer, String> {

        @Override
        public Class<Integer> sourceType() {
            return null;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public String encode(Integer fromJava) throws AchillesTranscodingException {
            return fromJava.toString();
        }

        @Override
        public Integer decode(String fromCassandra) throws AchillesTranscodingException {
            return Integer.parseInt(fromCassandra);
        }
    }

    public static class NullTargetTypeCodec implements Codec<Integer, String> {

        @Override
        public Class<Integer> sourceType() {
            return Integer.class;
        }

        @Override
        public Class<String> targetType() {
            return null;
        }

        @Override
        public String encode(Integer fromJava) throws AchillesTranscodingException {
            return fromJava.toString();
        }

        @Override
        public Integer decode(String fromCassandra) throws AchillesTranscodingException {
            return Integer.parseInt(fromCassandra);
        }
    }

    public static class NonMatchingSourceTypeCodec implements Codec<Integer, String> {

        @Override
        public Class<Integer> sourceType() {
            return Integer.class;
        }

        @Override
        public Class<String> targetType() {
            return String.class;
        }

        @Override
        public String encode(Integer fromJava) throws AchillesTranscodingException {
            return fromJava.toString();
        }

        @Override
        public Integer decode(String fromCassandra) throws AchillesTranscodingException {
            return Integer.parseInt(fromCassandra);
        }
    }

    public static class NonMatchingTargetTypeCodec implements Codec<Long, StringUtils> {

        @Override
        public Class<Long> sourceType() {
            return Long.class;
        }

        @Override
        public Class<StringUtils> targetType() {
            return StringUtils.class;
        }

        @Override
        public StringUtils encode(Long fromJava) throws AchillesTranscodingException {
            return null;
        }

        @Override
        public Long decode(StringUtils fromCassandra) throws AchillesTranscodingException {
            return null;
        }
    }
}