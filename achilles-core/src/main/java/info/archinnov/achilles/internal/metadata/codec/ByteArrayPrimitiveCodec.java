package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.nio.ByteBuffer;

public class ByteArrayPrimitiveCodec implements SimpleCodec<byte[],ByteBuffer> {

    @Override
    public Class<byte[]> sourceType() {
        return byte[].class;
    }

    @Override
    public Class<ByteBuffer> targetType() {
        return ByteBuffer.class;
    }

    @Override
    public ByteBuffer encode(byte[] fromJava) throws AchillesTranscodingException{
        if(fromJava == null) return null;
        return  ByteBuffer.wrap(fromJava);
    }

    @Override
    public byte[] decode(ByteBuffer fromCassandra) {
        if(fromCassandra == null) return null;
        return readByteBuffer(fromCassandra);
    }

    private byte[] readByteBuffer(Object fromCassandra) {
        ByteBuffer byteBuffer = (ByteBuffer) fromCassandra;
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }
}
