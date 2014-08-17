package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.Set;

public interface SetCodec<FROM,TO> {

    Class<FROM> sourceType();

    Class<TO> targetType();

    Set<TO> encode(Set<FROM> fromJava) throws AchillesTranscodingException;

    Set<FROM> decode(Set<TO> fromCassandra) throws AchillesTranscodingException;
}
