package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.SetTranscoder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import java.util.Set;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import com.google.common.collect.Sets;

/**
 * SetTranscoderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class SetTranscoderTest {

    private SetTranscoder transcoder = new SetTranscoder(mock(ObjectMapper.class));

    @Test
    public void should_encode() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        Set actual = transcoder.encode(pm, Sets.newHashSet("value"));

        assertThat(actual).containsExactly("value");
    }

    @Test
    public void should_decode() throws Exception
    {
        PropertyMeta pm = PropertyMetaTestBuilder
                .valueClass(String.class)
                .type(SIMPLE)
                .build();

        Set actual = transcoder.decode(pm, Sets.newHashSet("value"));

        assertThat(actual).containsExactly("value");
    }
}
