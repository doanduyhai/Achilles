package info.archinnov.achilles.internal.metadata.codec;

import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.internal.metadata.codec.NativeCodec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NativeCodecTest {

    @Test
    public void should_encode_and_decode() throws Exception {
        //Given
        NativeCodec<Long> codec = NativeCodec.create(Long.class);

        //When
        Long encoded = codec.encode(10L);
        Long decoded = codec.decode(11L);

        //Then
        assertThat(encoded).isEqualTo(10L);
        assertThat(decoded).isEqualTo(11L);
    }



}