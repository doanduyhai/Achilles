package info.archinnov.achilles.internal.metadata.transcoding.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.nio.ByteBuffer;

import static java.lang.String.format;

public class ByteCodec implements SimpleCodec<Byte,ByteBuffer> {

    @Override
    public Class<Byte> sourceType() {
        return Byte.class;
    }

    @Override
    public Class<ByteBuffer> targetType() {
        return ByteBuffer.class;
    }

    @Override
    public ByteBuffer encode(Byte fromJava) throws AchillesTranscodingException {
        if(fromJava == null) return  null;
        return ByteBuffer.wrap(new byte[] { fromJava });
    }

    @Override
    public Byte decode(ByteBuffer fromCassandra) throws AchillesTranscodingException {
        if(fromCassandra == null) return null;
        byte[] byteBuffer = readByteBuffer(fromCassandra);
        if (byteBuffer.length < 1) {
            throw new AchillesTranscodingException("Error while decoding value '" + fromCassandra + "' to type 'byte' ");
        }
        return byteBuffer[0];
    }

    private byte[] readByteBuffer(Object fromCassandra) {
        ByteBuffer byteBuffer = (ByteBuffer) fromCassandra;
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }
}
