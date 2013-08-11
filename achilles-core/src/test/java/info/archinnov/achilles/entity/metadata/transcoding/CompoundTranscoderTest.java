package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.entity.metadata.PropertyType.EMBEDDED_ID;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * CompoundTranscoderTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CompoundTranscoderTest {

    private CompoundTranscoder transcoder;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ReflectionInvoker invoker;

    @Before
    public void setUp()
    {
        transcoder = new CompoundTranscoder(objectMapper);
    }

    @Test
    public void should_encode_to_components() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        String name = "name";
        CompoundKey compound = new CompoundKey(userId, name);

        Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
        Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(EMBEDDED_ID)
                .compClasses(Long.class, String.class)
                .compGetters(userIdGetter, nameGetter)
                .build();

        when(invoker.getValueFromField(compound, userIdGetter)).thenReturn(userId);
        when(invoker.getValueFromField(compound, nameGetter)).thenReturn(name);

        List<Object> actual = transcoder.encodeToComponents(pm, compound);

        assertThat(actual).containsExactly(userId, name);
    }

    @Test
    public void should_encode_components() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        String name = "name";

        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(EMBEDDED_ID)
                .compClasses(Long.class, String.class, PropertyType.class)
                .build();

        List<Object> actual = transcoder.encodeComponents(pm,
                Arrays.<Object> asList(userId, PropertyType.EMBEDDED_ID, name));

        assertThat(actual).containsExactly(userId, "EMBEDDED_ID", name);
    }

    @Test
    public void should_decode_from_components_with_injection_by_setters() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        String name = "name";

        Method userIdSetter = CompoundKey.class.getDeclaredMethod("setUserId", Long.class);
        Method namesetter = CompoundKey.class.getDeclaredMethod("setName", String.class);

        Constructor<CompoundKey> constructor = CompoundKey.class.getDeclaredConstructor();

        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(EMBEDDED_ID)
                .compClasses(Long.class, String.class)
                .compSetters(userIdSetter, namesetter)
                .build();

        pm.getEmbeddedIdProperties().setConstructor(constructor);

        Object actual = transcoder.decodeFromComponents(pm, Arrays.<Object> asList(userId, name));

        assertThat(actual).isInstanceOf(CompoundKey.class);

        CompoundKey compound = (CompoundKey) actual;

        assertThat(compound.getUserId()).isEqualTo(userId);
        assertThat(compound.getName()).isEqualTo(name);
    }

    @Test
    public void should_decode_from_components_with_injection_by_constructor() throws Exception
    {
        Long userId = RandomUtils.nextLong();
        String name = "name";

        Constructor<CompoundKey> constructor = CompoundKey.class.getDeclaredConstructor(Long.class, String.class);

        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(CompoundKey.class)
                .type(EMBEDDED_ID)
                .compClasses(Long.class, String.class)
                .build();

        pm.getEmbeddedIdProperties().setConstructor(constructor);

        Object actual = transcoder.decodeFromComponents(pm, Arrays.<Object> asList(userId, name));

        assertThat(actual).isInstanceOf(CompoundKey.class);

        CompoundKey compound = (CompoundKey) actual;

        assertThat(compound.getUserId()).isEqualTo(userId);
        assertThat(compound.getName()).isEqualTo(name);
    }
}
