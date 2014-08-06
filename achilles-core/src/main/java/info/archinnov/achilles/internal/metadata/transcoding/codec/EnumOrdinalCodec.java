package info.archinnov.achilles.internal.metadata.transcoding.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.List;

import static java.lang.String.format;

public class EnumOrdinalCodec<ENUM> implements SimpleCodec<ENUM,Integer> {

    private final List<ENUM> enumValues;
    private final Class<ENUM> sourceType;

    public EnumOrdinalCodec(List<ENUM> enumValues, Class<ENUM> sourceType) {
        this.enumValues = enumValues;
        this.sourceType = sourceType;
    }

    @Override
    public Class<ENUM> sourceType() {
        return sourceType;
    }

    @Override
    public Class<Integer> targetType() {
        return Integer.class;
    }

    @Override
    public Integer encode(ENUM fromJava) {
        if(fromJava == null) return null;
        if (!fromJava.getClass().isEnum()) {
            throw new AchillesTranscodingException(format("Object '%s' to be encoded should be an enum", fromJava));
        }
        for (int i = 0; i < enumValues.size(); i++) {
            if (enumValues.get(i) == fromJava) {
                return i;
            }
        }
        throw new AchillesTranscodingException(format("Cannot find matching enum values for '%s' from possible enum constants '%s' ", fromJava, enumValues));
    }

    @Override
    public ENUM decode(Integer fromCassandra) {
        if(fromCassandra == null) return null;
        if (fromCassandra > enumValues.size()-1 || fromCassandra < 0) {
            throw new AchillesTranscodingException(format("Cannot find matching enum values for '%s' from possible enum constants '%s' ", fromCassandra, enumValues));
        }
        return enumValues.get(fromCassandra);
    }

    public static <TYPE> EnumOrdinalCodec<TYPE> create(List<TYPE> enumValues, Class<TYPE> sourceType) {
        return new EnumOrdinalCodec<>(enumValues, sourceType);
    }
}
