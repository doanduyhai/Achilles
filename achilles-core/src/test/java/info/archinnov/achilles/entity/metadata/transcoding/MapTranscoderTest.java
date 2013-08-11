package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;

/**
 * MapTranscoderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapTranscoderTest {

    private MapTranscoder transcoder = new MapTranscoder(mock(ObjectMapper.class));

    @Test
    public void should_encode() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, String.class)
                .type(SIMPLE)
                .build();

        Map actual = transcoder.encode(pm, ImmutableMap.of(1, "value"));

        assertThat(actual).containsKey(1);
        assertThat(actual).containsValue("value");
    }

    @Test
    public void should_encode_key() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, String.class)
                .type(JOIN_SIMPLE)
                .build();

        assertThat(transcoder.encodeKey(pm, 11)).isEqualTo(11);
    }

    @Test
    public void should_encode_value() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, String.class)
                .type(SIMPLE)
                .build();

        assertThat(transcoder.encode(pm, "value")).isEqualTo("value");
    }

    @Test
    public void should_decode() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, String.class)
                .type(SIMPLE)
                .build();

        Map actual = transcoder.decode(pm, ImmutableMap.of(1, "value"));

        assertThat(actual).containsKey(1);
        assertThat(actual).containsValue("value");
    }

    @Test
    public void should_decode_key() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, String.class)
                .type(JOIN_SIMPLE)
                .build();

        assertThat(transcoder.decodeKey(pm, 11)).isEqualTo(11);
    }

    @Test
    public void should_decode_value() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .keyValueClass(Integer.class, String.class)
                .type(SIMPLE)
                .build();

        assertThat(transcoder.decode(pm, "value")).isEqualTo("value");
    }

}
