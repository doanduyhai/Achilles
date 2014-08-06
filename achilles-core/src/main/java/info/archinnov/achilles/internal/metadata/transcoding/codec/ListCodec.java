package info.archinnov.achilles.internal.metadata.transcoding.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.List;

public interface ListCodec<FROM,TO> {

    Class<FROM> sourceType();

    Class<TO> targetType();

    List<TO> encode(List<FROM> fromJava) throws AchillesTranscodingException;

    List<FROM> decode(List<TO> fromCassandra) throws AchillesTranscodingException;
}
