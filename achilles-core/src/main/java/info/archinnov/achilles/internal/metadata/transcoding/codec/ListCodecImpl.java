package info.archinnov.achilles.internal.metadata.transcoding.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class ListCodecImpl<FROM,TO> implements ListCodec<FROM,TO> {

    private final Class<FROM> sourceType;
    private final Class<TO> targetType;
    private final SimpleCodec<FROM,TO> valueCodec;

    public ListCodecImpl(Class<FROM> sourceType, Class<TO> targetType, SimpleCodec<FROM, TO> valueCodec) {
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
    public List<TO> encode(List<FROM> fromJava) throws AchillesTranscodingException {
        if(fromJava == null) return null;
        List<TO> encoded = new ArrayList<>();
        for (FROM source : fromJava) {
            encoded.add(valueCodec.encode(source));
        }
        return encoded;
    }

    @Override
    public List<FROM> decode(List<TO> fromCassandra) throws AchillesTranscodingException {
        if(fromCassandra == null) return null;
        List<FROM> decoded = new ArrayList<>();
        for (TO source : fromCassandra) {
            decoded.add(valueCodec.decode(source));
        }
        return decoded;
    }


}
