package info.archinnov.achilles.internal.metadata.transcoding.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.exception.AchillesTranscodingException;

import java.io.IOException;

import static java.lang.String.format;

public class JSONCodec<TYPE> implements SimpleCodec<TYPE,String> {

    private final Class<TYPE> sourceType;
    private final ObjectMapper objectMapper;

    public JSONCodec(ObjectMapper objectMapper, Class<TYPE> sourceType) {
        this.sourceType = sourceType;
        this.objectMapper = objectMapper;
    }

    @Override
    public Class<TYPE> sourceType() {
        return sourceType;
    }

    @Override
    public Class<String> targetType() {
        return String.class;
    }

    @Override
    public String encode(TYPE fromJava) throws AchillesTranscodingException {
        if(fromJava == null) return null;
        try {
            return objectMapper.writeValueAsString(fromJava);
        } catch (JsonProcessingException e) {
            throw new AchillesTranscodingException(e);
        }
    }

    @Override
    public TYPE decode(String fromCassandra) throws AchillesTranscodingException {
        if(fromCassandra == null) return null;
        try {
            return objectMapper.readValue(fromCassandra, sourceType);
        } catch (IOException e) {
            throw new AchillesTranscodingException(e);
        }
    }

    public static <TYPE> JSONCodec<TYPE> create(ObjectMapper objectMapper, Class<TYPE> sourceType) {
        return new JSONCodec<>(objectMapper, sourceType);
    }
}
