package info.archinnov.achilles.serializer;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static org.fest.assertions.api.Assertions.*;
import integration.tests.entity.CompoundKeyWithEnum;
import integration.tests.entity.User;
import java.io.Serializable;
import org.junit.Test;

public class ThriftSerializerTypeInfererTest {

    @Test
    public void should_return_string_serializer_for_non_native_class() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<String> getSerializer(User.class)).isEqualTo(
                ThriftSerializerUtils.STRING_SRZ);
    }

    @Test
    public void should_return_string_serializer_for_enum_class() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<String> getSerializer(CompoundKeyWithEnum.Type.class)).isInstanceOf(
                ThriftEnumSerializer.class);
    }

    @Test
    public void should_return_string_serializer_for_supported_class() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<Long> getSerializer(Long.class)).isEqualTo(LONG_SRZ);
    }

    @Test
    public void should_return_string_serializer_for_serializable_type() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<String> getSerializer(Pojo.class)).isEqualTo(STRING_SRZ);
    }

    @Test
    public void should_return_string_serializer_for_object_instance() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<Long> getSerializer(10L)).isEqualTo(LONG_SRZ);
    }

    @Test
    public void should_return_null_for_null_class_param() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<Long> getSerializer(null)).isNull();
    }

    @Test
    public void should_return_null_for_null_object_param() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<Long> getSerializer((Object) null)).isNull();
    }

    public static class Pojo implements Serializable
    {
        private static final long serialVersionUID = 1L;

    }
}
