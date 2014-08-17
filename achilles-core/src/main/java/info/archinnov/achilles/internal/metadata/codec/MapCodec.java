package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.Map;

public interface MapCodec<FROM_KEY,FROM_VAL,TO_KEY,TO_VAL> {

    Class<FROM_KEY> sourceKeyType();

    Class<FROM_VAL> sourceValueType();

    Class<TO_KEY> targetKeyType();

    Class<TO_VAL> targetValueType();

    Map<TO_KEY,TO_VAL> encode(Map<FROM_KEY,FROM_VAL> fromJava) throws AchillesTranscodingException;

    Map<FROM_KEY,FROM_VAL> decode(Map<TO_KEY,TO_VAL> fromCassandra) throws AchillesTranscodingException;
}
