package info.archinnov.achilles.internal.metadata.transcoding.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.nio.ByteBuffer;

public class ByteArrayCodec implements SimpleCodec<Byte[],ByteBuffer> {

    @Override
    public Class<Byte[]> sourceType() {
        return Byte[].class;
    }

    @Override
    public Class<ByteBuffer> targetType() {
        return ByteBuffer.class;
    }

    @Override
    public ByteBuffer encode(Byte[] fromJava) throws AchillesTranscodingException{
        if(fromJava == null) return null;
        byte[] bytesPrimitive = new byte[fromJava.length];
        int i=0;
        for(byte b: fromJava) bytesPrimitive[i++] = b;
        return  ByteBuffer.wrap(bytesPrimitive);
    }

    @Override
    public Byte[] decode(ByteBuffer fromCassandra) {
        if(fromCassandra == null) return null;
        return readByteBuffer(fromCassandra);
    }

    private Byte[] readByteBuffer(Object fromCassandra) {
        ByteBuffer byteBuffer = (ByteBuffer) fromCassandra;
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        Byte[] byteObjects = new Byte[bytes.length];
        int i=0;
        for(byte b: bytes) byteObjects[i++] = b;
        return byteObjects;
    }
}
