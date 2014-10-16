package info.archinnov.achilles.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

public class IdentityCodec implements Codec<Object,Object> {
    @Override
    public Class<Object> sourceType() {
        return Object.class;
    }

    @Override
    public Class<Object> targetType() {
        return Object.class;
    }

    @Override
    public Object encode(Object fromJava) throws AchillesTranscodingException {
        return fromJava;
    }

    @Override
    public Object decode(Object fromCassandra) throws AchillesTranscodingException {
        return fromCassandra;
    }
}
