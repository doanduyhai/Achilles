package info.archinnov.achilles.internal.metadata.codec;

import static org.fest.assertions.api.Assertions.*;

import info.archinnov.achilles.internal.metadata.codec.ByteArrayCodec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class ByteArrayCodecTest {

    @Test
    public void should_encoded_and_decode() throws Exception {
        //Given
        Byte[] byteArray = new Byte[]{(byte)2, (byte)5};
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{(byte) 2, (byte) 5});

        ByteArrayCodec codec = new ByteArrayCodec();

        //When
        ByteBuffer encoded = codec.encode(byteArray);
        Byte[] decoded = codec.decode(byteBuffer);

        //Then
        assertThat(encoded.array()).contains((byte) 2, (byte) 5);
        assertThat(decoded).contains((byte)2, (byte)5);
    }

}