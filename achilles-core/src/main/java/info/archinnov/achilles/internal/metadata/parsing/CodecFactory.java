package info.archinnov.achilles.internal.metadata.parsing;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import info.archinnov.achilles.annotations.Enumerated;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import info.archinnov.achilles.internal.metadata.transcoding.codec.*;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static info.archinnov.achilles.annotations.Enumerated.Encoding;

public class CodecFactory {

    private PropertyFilter filter = new PropertyFilter();

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

    SimpleCodec parseSimpleField(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        final Class type = field.getType();
        final Optional<Encoding> maybeEncoding = fromNullable(field.getAnnotation(Enumerated.class)).transform(valueEncoding);
        return createSimpleCodec(context, type, maybeEncoding);
    }

    ListCodec parseListField(PropertyParsingContext context) {
        final SimpleCodec simpleCodec = createSimpleCodecForCollection(context);
        return new ListCodecImpl(simpleCodec.sourceType(), simpleCodec.targetType(), simpleCodec);
    }

    SetCodec parseSetField(PropertyParsingContext context) {
        final SimpleCodec simpleCodec = createSimpleCodecForCollection(context);
        return new SetCodecImpl(simpleCodec.sourceType(), simpleCodec.targetType(), simpleCodec);
    }

    MapCodec parseMapField(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        final Optional<Encoding> maybeEncodingKey = fromNullable(field.getAnnotation(Enumerated.class)).transform(keyEncoding);
        final Optional<Encoding> maybeEncodingValue = fromNullable(field.getAnnotation(Enumerated.class)).transform(valueEncoding);

        final Pair<Class<Object>, Class<Object>> sourceTargetTypes = TypeParser.determineMapGenericTypes(field);

        final SimpleCodec keyCodec = createSimpleCodec(context, sourceTargetTypes.left, maybeEncodingKey);
        final SimpleCodec valueCodec = createSimpleCodec(context, sourceTargetTypes.right, maybeEncodingValue);

        return MapCodecBuilder.fromKeyType(keyCodec.sourceType())
                .toKeyType(keyCodec.targetType())
                .withKeyCodec(keyCodec)
                .fromValueType(valueCodec.sourceType())
                .toValueType(valueCodec.targetType())
                .withValueCodec(valueCodec);
    }

    private SimpleCodec createSimpleCodec(PropertyParsingContext context, Class type, Optional<Encoding> maybeEncoding) {
        SimpleCodec codec;
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
        } else {
            codec = new JSONCodec<>(context.getCurrentObjectMapper(), type);
        }
        return codec;
    }

    private SimpleCodec createEnumCodec(Class type, Optional<Encoding> maybeEncoding) {
        SimpleCodec codec;
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

    private SimpleCodec createSimpleCodecForCollection(PropertyParsingContext context) {
        final Field field = context.getCurrentField();
        final Optional<Encoding> maybeEncoding = fromNullable(field.getAnnotation(Enumerated.class)).transform(valueEncoding);
        final Class<Object> valueType = TypeParser.inferValueClassForListOrSet(field.getGenericType(), field.getClass());
        return createSimpleCodec(context, valueType, maybeEncoding);
    }
}
