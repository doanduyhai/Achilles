package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

/**
 * SimpleTranscoderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class SimpleTranscoderTest {

    private SimpleTranscoder transcoder = new SimpleTranscoder(mock(ObjectMapper.class));

    @Test
    public void should_encode() throws Exception
    {
        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        Object actual = transcoder.encode(pm, "value");

        assertThat(actual).isEqualTo("value");
    }

    @Test
    public void should_decode() throws Exception
    {
        PropertyMeta<Void, String> pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        Object actual = transcoder.decode(pm, "value");

        assertThat(actual).isEqualTo("value");
    }
}
