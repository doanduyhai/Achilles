package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.ListTranscoder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import java.util.Arrays;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

/**
 * ListTranscoderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class ListTranscoderTest {

    private ListTranscoder transcoder = new ListTranscoder(mock(ObjectMapper.class));

    @Test
    public void should_encode() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        List actual = transcoder.encode(pm, Arrays.asList("value"));

        assertThat(actual).containsExactly("value");
    }

    @Test
    public void should_decode() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        List actual = transcoder.decode(pm, Arrays.asList("value"));

        assertThat(actual).containsExactly("value");
    }
}
