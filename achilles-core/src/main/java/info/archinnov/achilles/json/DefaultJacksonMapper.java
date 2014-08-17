package info.archinnov.achilles.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

public enum DefaultJacksonMapper {
    DEFAULT(defaultJacksonMapper()),
    COUNTER(defaultJacksonMapperForCounterKey());

    private final ObjectMapper jacksonMapper;


    DefaultJacksonMapper(ObjectMapper jacksonMapper) {
        this.jacksonMapper = jacksonMapper;
    }

    public ObjectMapper get() {
        return jacksonMapper;
    }

    private static ObjectMapper defaultJacksonMapper() {
        ObjectMapper defaultMapper = new ObjectMapper();
        defaultMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        defaultMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
        AnnotationIntrospector secondary = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
        defaultMapper.setAnnotationIntrospector(AnnotationIntrospector.pair(primary, secondary));
        return defaultMapper;
    }

    private static ObjectMapper defaultJacksonMapperForCounterKey() {
        ObjectMapper defaultMapper = new ObjectMapper();
        defaultMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        defaultMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        defaultMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        AnnotationIntrospector primary = new JacksonAnnotationIntrospector();
        AnnotationIntrospector secondary = new JaxbAnnotationIntrospector(TypeFactory.defaultInstance());
        defaultMapper.setAnnotationIntrospector(AnnotationIntrospector.pair(primary, secondary));
        return defaultMapper;
    }
}
