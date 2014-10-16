package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.HashSet;
import java.util.Set;

public class SetCodecImpl<FROM,TO> implements SetCodec<FROM,TO> {

    private final Class<FROM> sourceType;
    private final Class<TO> targetType;
    private final Codec<FROM,TO> valueCodec;

    public SetCodecImpl(Class<FROM> sourceType, Class<TO> targetType, Codec<FROM, TO> valueCodec) {
        this.sourceType = sourceType;
        this.targetType = targetType;
        this.valueCodec = valueCodec;
    }

    @Override
    public Class<FROM> sourceType() {
        return sourceType;
    }

    @Override
    public Class<TO> targetType() {
        return targetType;
    }

    @Override
    public Set<TO> encode(Set<FROM> fromJava) throws AchillesTranscodingException {
        if(fromJava == null) return null;
        Set<TO> encoded = new HashSet<>();
        for (FROM source : fromJava) {
            encoded.add(valueCodec.encode(source));
        }
        return encoded;
    }

    @Override
    public Set<FROM> decode(Set<TO> fromCassandra) throws AchillesTranscodingException {
        if(fromCassandra == null) return null;
        Set<FROM> decoded = new HashSet<>();
        for (TO source : fromCassandra) {
            decoded.add(valueCodec.decode(source));
        }
        return decoded;
    }
}
