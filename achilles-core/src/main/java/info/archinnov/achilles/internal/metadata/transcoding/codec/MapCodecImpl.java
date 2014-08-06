package info.archinnov.achilles.internal.metadata.transcoding.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.HashMap;
import java.util.Map;

public class MapCodecImpl<FROM_KEY,FROM_VAL,TO_KEY,TO_VAL> implements MapCodec<FROM_KEY,FROM_VAL,TO_KEY,TO_VAL>{

    private final Class<FROM_KEY> sourceKeyType;
    private final Class<FROM_VAL> sourceValueType;
    private final Class<TO_KEY> targetKeyType;
    private final Class<TO_VAL> targetValueType;
    private final SimpleCodec<FROM_KEY,TO_KEY> keyCodec;
    private final SimpleCodec<FROM_VAL,TO_VAL> valueCodec;

    MapCodecImpl(Class<FROM_KEY> sourceKeyType, Class<FROM_VAL> sourceValueType, Class<TO_KEY> targetKeyType, Class<TO_VAL> targetValueType, SimpleCodec<FROM_KEY, TO_KEY> keyCodec, SimpleCodec<FROM_VAL, TO_VAL> valueCodec) {
        this.sourceKeyType = sourceKeyType;
        this.sourceValueType = sourceValueType;
        this.targetKeyType = targetKeyType;
        this.targetValueType = targetValueType;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public Class<FROM_KEY> sourceKeyType() {
        return sourceKeyType;
    }

    @Override
    public Class<FROM_VAL> sourceValueType() {
        return sourceValueType;
    }

    @Override
    public Class<TO_KEY> targetKeyType() {
        return targetKeyType;
    }

    @Override
    public Class<TO_VAL> targetValueType() {
        return targetValueType;
    }

    @Override
    public Map<TO_KEY, TO_VAL> encode(Map<FROM_KEY, FROM_VAL> fromJava) throws AchillesTranscodingException {
        if(fromJava == null) return null;
        Map<TO_KEY, TO_VAL> encoded = new HashMap<>();
        for (Map.Entry<FROM_KEY,FROM_VAL> source : fromJava.entrySet()) {
            encoded.put(keyCodec.encode(source.getKey()), valueCodec.encode(source.getValue()));
        }
        return encoded;
    }

    @Override
    public Map<FROM_KEY, FROM_VAL> decode(Map<TO_KEY, TO_VAL> fromCassandra) throws AchillesTranscodingException {
        if(fromCassandra == null) return null;
        Map<FROM_KEY, FROM_VAL>decoded = new HashMap<>();
        for (Map.Entry<TO_KEY, TO_VAL> source : fromCassandra.entrySet()) {
            decoded.put(keyCodec.decode(source.getKey()), valueCodec.decode(source.getValue()));
        }
        return decoded;
    }
}
