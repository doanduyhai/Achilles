package info.archinnov.achilles.internal.metadata.codec;

import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.internal.metadata.codec.ByteCodec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class ByteCodecTest {

    @Test
    public void should_encode_and_decode() throws Exception {
        //Given
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{5});
        ByteCodec codec = new ByteCodec();

        //When
        ByteBuffer encoded = codec.encode((byte) 3);
        Byte decoded = codec.decode(byteBuffer);

        //Then
        assertThat(encoded.array()[0]).isEqualTo((byte)3);
        assertThat(decoded).isEqualTo(new Byte((byte)5));
    }

}