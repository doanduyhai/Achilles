package info.archinnov.achilles.internal.metadata.transcoding.codec;

import static org.fest.assertions.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class ByteArrayPrimitiveCodecTest {

    @Test
    public void should_encoded_and_decode() throws Exception {
        //Given
        byte[] byteArray = new byte[]{(byte)2, (byte)5};
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{(byte) 2, (byte) 5});

        ByteArrayPrimitiveCodec codec = new ByteArrayPrimitiveCodec();

        //When
        ByteBuffer encoded = codec.encode(byteArray);
        byte[] decoded = codec.decode(byteBuffer);

        //Then
        assertThat(encoded.array()).contains((byte) 2, (byte) 5);
        assertThat(decoded).contains((byte)2, (byte)5);
    }
}