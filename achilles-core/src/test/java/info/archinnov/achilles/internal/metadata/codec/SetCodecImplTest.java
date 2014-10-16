package info.archinnov.achilles.internal.metadata.codec;

import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.codec.Codec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class SetCodecImplTest {

    @Test
    public void should_encode_and_decode() throws Exception {
        //Given
        Codec<Integer, String> valueCodec = new Codec<Integer, String>() {
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

        SetCodec<Integer, String> codec = SetCodecBuilder
                .fromType(Integer.class)
                .toType(String.class)
                .withCodec(valueCodec);

        //When
        Set<String> encoded = codec.encode(Sets.newSet(1, 2));
        Set<Integer> decoded = codec.decode(Sets.newSet("3", "4"));

        //Then
        assertThat(encoded).containsOnly("1", "2");
        assertThat(decoded).containsOnly(3, 4);
    }

}