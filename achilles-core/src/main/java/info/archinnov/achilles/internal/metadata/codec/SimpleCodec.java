package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

public interface SimpleCodec<FROM extends Object,TO extends Object> {

    Class<FROM> sourceType();

    Class<TO> targetType();

    TO encode(FROM fromJava) throws AchillesTranscodingException;

    FROM decode(TO fromCassandra) throws AchillesTranscodingException;
}
