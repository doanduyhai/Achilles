package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.util.List;

import static java.lang.String.format;

public class EnumNameCodec<ENUM> implements SimpleCodec<ENUM,String> {

    private final List<ENUM> enumValues;
    private final Class<ENUM> sourceType;

    public EnumNameCodec(List<ENUM> enumValues, Class<ENUM> sourceType) {
        this.enumValues = enumValues;
        this.sourceType = sourceType;
    }

    @Override
    public Class<ENUM> sourceType() {
        return sourceType;
    }

    @Override
    public Class<String> targetType() {
        return String.class;
    }

    @Override
    public String encode(ENUM fromJava) throws AchillesTranscodingException {
        if(fromJava == null) return null;
        if (!fromJava.getClass().isEnum()) {
            throw new AchillesTranscodingException(format("Object '%s' to be encoded should be an enum", fromJava));
        }
        if (!enumValues.contains(fromJava)) {
            throw new AchillesTranscodingException(format("Cannot find matching enum values for '%s' from possible enum constants '%s' ", fromJava, enumValues));
        }
        return ((Enum<?>) fromJava).name();
    }

    @Override
    public ENUM decode(String fromCassandra) throws AchillesTranscodingException {
        if(fromCassandra == null) return null;
        for (ENUM enumValue : enumValues) {
            if(((Enum<?>)enumValue).name().equals(fromCassandra)) return enumValue;
        }
        throw new AchillesTranscodingException(format("Cannot find matching enum values for '%s' from possible enum constants '%s' ", fromCassandra, enumValues));
    }

    public static <TYPE> EnumNameCodec<TYPE> create(List<TYPE> enumTypes, Class<TYPE> sourceType) {
        return new EnumNameCodec<>(enumTypes, sourceType);
    }


}
