package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesTranscodingException;

import static java.lang.String.format;

public class NativeCodec<TYPE> implements Codec<TYPE,TYPE> {

    private final Class<TYPE> type;

    public NativeCodec(Class<TYPE> type) {
        this.type = type;
    }

    @Override
    public Class<TYPE> sourceType() {
        return type;
    }

    @Override
    public Class<TYPE> targetType() {
        return type;
    }

    @Override
    public TYPE encode(TYPE fromJava) throws AchillesTranscodingException {
        return fromJava;
    }

    @Override
    public TYPE decode(TYPE fromCassandra) throws AchillesTranscodingException {
        return fromCassandra;
    }

    public static <T> NativeCodec<T> create(Class<T> type) {
        return new NativeCodec<>(type);
    }
}
