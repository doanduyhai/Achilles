package info.archinnov.achilles.internal.metadata.transcoding.codec;

import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class ListCodecImplTest {

    @Test
    public void should_encode_and_decode() throws Exception {
        //Given
        SimpleCodec<Integer, String> valueCodec = new SimpleCodec<Integer, String>() {
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
        };

        ListCodec<Integer, String> codec = ListCodecBuilder
                .fromType(Integer.class)
                .toType(String.class)
                .withCodec(valueCodec);

        //When
        List<String> encoded = codec.encode(Arrays.asList(1, 2));
        List<Integer> decoded = codec.decode(Arrays.asList("3", "4"));

        //Then
        assertThat(encoded).containsExactly("1", "2");
        assertThat(decoded).containsExactly(3, 4);
    }


}