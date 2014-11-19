package info.archinnov.achilles.internal.metadata.parsing;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.annotations.JSON;
import info.archinnov.achilles.annotations.TypeTransformer;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.codec.ByteArrayCodec;
import info.archinnov.achilles.internal.metadata.codec.ByteArrayPrimitiveCodec;
import info.archinnov.achilles.internal.metadata.codec.ByteCodec;
import info.archinnov.achilles.internal.metadata.codec.EnumNameCodec;
import info.archinnov.achilles.internal.metadata.codec.EnumOrdinalCodec;
import info.archinnov.achilles.internal.metadata.codec.JSONCodec;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.ListCodecImpl;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodecBuilder;
import info.archinnov.achilles.internal.metadata.codec.NativeCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodecImpl;
import info.archinnov.achilles.internal.metadata.holder.InternalTimeUUID;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.annotations.Enumerated.Encoding;
import static java.lang.String.format;

public class CodecFactory {

    private static final Logger log = LoggerFactory.getLogger(CodecFactory.class);

    private static final Function<Enumerated, Encoding> keyEncoding = new Function<Enumerated, Encoding>() {
        @Override
        public Encoding apply(Enumerated input) {
            return input.key();
        }
    };

    private static final Function<Enumerated, Encoding> valueEncoding = new Function<Enumerated, Encoding>() {
        @Override
        public Encoding apply(Enumerated input) {
            return input.value();
        }
    };

    protected PropertyFilter filter = PropertyFilter.Singleton.INSTANCE.get();
    protected TypeTransformerParser typeTransformerParser = TypeTransformerParser.Singleton.INSTANCE.get();

    Codec parseSimpleField(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        log.debug("Parse simple codec for field {}", field);
        final Class type = field.getType();
        if (filter.hasAnnotation(field, TypeTransformer.class)) {
            return typeTransformerParser.parseAndValidateSimpleCodec(field);
        } else {
            final Optional<Encoding> maybeEncoding = fromNullable(field.getAnnotation(Enumerated.class)).transform(valueEncoding);
            return createSimpleCodec(context, type, maybeEncoding);
        }
    }

    ListCodec parseListField(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        log.debug("Parse list codec for field {}", field);
        if (filter.hasAnnotation(field, TypeTransformer.class)) {
            return typeTransformerParser.parseAndValidateListCodec(field);
        } else {
            final Codec simpleCodec = createSimpleCodecForCollection(context);
            return new ListCodecImpl(simpleCodec.sourceType(), simpleCodec.targetType(), simpleCodec);
        }
    }

    SetCodec parseSetField(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        log.debug("Parse set codec for field {}", field);
        if (filter.hasAnnotation(field, TypeTransformer.class)) {
            return typeTransformerParser.parseAndValidateSetCodec(field);
        } else {
            final Codec simpleCodec = createSimpleCodecForCollection(context);
            return new SetCodecImpl(simpleCodec.sourceType(), simpleCodec.targetType(), simpleCodec);
        }
    }

    MapCodec parseMapField(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        log.debug("Parse map codec for field {}", field);

        if (filter.hasAnnotation(field, TypeTransformer.class)) {
            return typeTransformerParser.parseAndValidateMapCodec(field);
        } else {
            final Optional<Encoding> maybeEncodingKey = fromNullable(field.getAnnotation(Enumerated.class)).transform(keyEncoding);
            final Optional<Encoding> maybeEncodingValue = fromNullable(field.getAnnotation(Enumerated.class)).transform(valueEncoding);

            final Pair<Class<Object>, Class<Object>> sourceTargetTypes = TypeParser.determineMapGenericTypes(field);

            final Codec keyCodec = createSimpleCodec(context, sourceTargetTypes.left, maybeEncodingKey);
            final Codec valueCodec = createSimpleCodec(context, sourceTargetTypes.right, maybeEncodingValue);

            return MapCodecBuilder.fromKeyType(keyCodec.sourceType())
                    .toKeyType(keyCodec.targetType())
                    .withKeyCodec(keyCodec)
                    .fromValueType(valueCodec.sourceType())
                    .toValueType(valueCodec.targetType())
                    .withValueCodec(valueCodec);
        }

    }

    Class<?> determineCQL3ValueType(Codec simpleCodec, boolean timeUUID) {
        log.trace("Determine CQL3 type for type {}", simpleCodec.sourceType());
        return determineType(simpleCodec.targetType(), timeUUID);
    }

    Class<?> determineCQL3ValueType(ListCodec listCodec, boolean timeUUID) {
        log.trace("Determine CQL3 type for list type {}", listCodec.sourceType());
        return determineType(listCodec.targetType(), timeUUID);
    }

    Class<?> determineCQL3ValueType(SetCodec setCodec, boolean timeUUID) {
        log.trace("Determine CQL3 type for set type {}", setCodec.sourceType());
        return determineType(setCodec.targetType(), timeUUID);
    }

    Class<?> determineCQL3ValueType(MapCodec mapCodec, boolean timeUUID) {
        log.trace("Determine CQL3 type for map type {}", mapCodec.sourceValueType());
        return determineType(mapCodec.targetValueType(), timeUUID);
    }


    Class<?> determineCQL3KeyType(MapCodec mapCodec, boolean timeUUID) {
        log.trace("Determine CQL3 type for type {}", mapCodec.sourceKeyType());
        return determineType(mapCodec.targetKeyType(), timeUUID);
    }

    private Class<?> determineType(Class<?> input, boolean timeUUID) {
        if (timeUUID && UUID.class.isAssignableFrom(input)) {
            return InternalTimeUUID.class;
        } else if (ByteBuffer.class.isAssignableFrom(input)) {
            return ByteBuffer.class;
        } else if (Counter.class == input) {
            return Long.class;
        } else {
            return input;
        }
    }
    private Codec createSimpleCodec(PropertyParsingContext context, Class type, Optional<Encoding> maybeEncoding) {
        log.debug("Create simple codec for java type {}", type);
        Codec codec;
        if (Byte.class.isAssignableFrom(type) || byte.class.isAssignableFrom(type)) {
            codec = new ByteCodec();
        } else if (byte[].class.isAssignableFrom(type)) {
            codec = new ByteArrayPrimitiveCodec();
        } else if (Byte[].class.isAssignableFrom(type)) {
            codec = new ByteArrayCodec();
        } else if (PropertyParser.isAssignableFromNativeType(type)) {
            codec = new NativeCodec<Object>(type);
        } else if (type.isEnum()) {
            codec = createEnumCodec(type, maybeEncoding);
        } else if (filter.hasAnnotation(context.getCurrentField(), JSON.class)) {
            codec = new JSONCodec<>(context.getCurrentObjectMapper(), type);
        } else {
            throw new AchillesBeanMappingException(format("The type '%s' on field '%s' of entity '%s' is not supported. If you want to convert it to JSON string, do not forget to add @JSON", type.getCanonicalName(), context.getCurrentPropertyName(), context.getCurrentEntityClass().getCanonicalName()));
        }
        return codec;
    }

    private Codec createEnumCodec(Class type, Optional<Encoding> maybeEncoding) {
        log.debug("Create enum codec for java type {}", type);
        Codec codec;
        final List<Object> enumConstants = Arrays.asList(type.getEnumConstants());
        if (maybeEncoding.isPresent()) {
            if (maybeEncoding.get() == Encoding.NAME) {
                codec = new EnumNameCodec<>(enumConstants, type);
            } else {
                codec = new EnumOrdinalCodec<>(enumConstants, type);
            }
        } else {
            codec = new EnumNameCodec<>(enumConstants, type);
        } return codec;
    }

    private Codec createSimpleCodecForCollection(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        final Optional<Encoding> maybeEncoding = fromNullable(field.getAnnotation(Enumerated.class)).transform(valueEncoding);
        final Class<Object> valueType = TypeParser.inferValueClassForListOrSet(field.getGenericType(), field.getClass());
        return createSimpleCodec(context, valueType, maybeEncoding);
    }

    public static enum Singleton {
        INSTANCE;

        private final CodecFactory instance = new CodecFactory();

        public CodecFactory get() {
            return instance;
        }
    }
}
