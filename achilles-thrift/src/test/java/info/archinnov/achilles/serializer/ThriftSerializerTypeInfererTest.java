package info.archinnov.achilles.serializer;

import static org.fest.assertions.api.Assertions.*;
import integration.tests.entity.CompoundKeyWithEnum;
import integration.tests.entity.User;
import org.junit.Test;

public class ThriftSerializerTypeInfererTest {

    @Test
    public void should_return_string_serializer_for_non_native_class() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<String> getSerializer(User.class)).isEqualTo(
                ThriftSerializerUtils.STRING_SRZ);
    }

    @Test
    public void should_return_string_serializer_for_enum_class() throws Exception {
        assertThat(ThriftSerializerTypeInferer.<String> getSerializer(CompoundKeyWithEnum.Type.class)).isEqualTo(
                ThriftSerializerUtils.STRING_SRZ);
    }
}
