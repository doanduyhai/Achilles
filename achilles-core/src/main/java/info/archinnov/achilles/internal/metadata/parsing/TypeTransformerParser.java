package info.archinnov.achilles.internal.metadata.parsing;

import info.archinnov.achilles.annotations.TypeTransformer;
import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.codec.IdentityCodec;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.ListCodecBuilder;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodecBuilder;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodecBuilder;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.internal.utils.Pair;

import java.lang.reflect.Field;

import static java.lang.String.format;

public class TypeTransformerParser {

    public Codec parseAndValidateSimpleCodec(Field field) {
        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        Codec<?, ?> codec = getValueCodecInstance(field);

        validateNotIdentityCodec(fieldName, className, codec);

        final Class<?> sourceType = codec.sourceType();
        final Class<?> targetType = codec.targetType();

        validateTypesNotNull(fieldName, className, sourceType, targetType);

        validateMatchingSourceType(fieldName, className, sourceType, field.getType());

        validateSupportedTargetType(fieldName, className, targetType);

        return codec;
    }

    public ListCodec parseAndValidateListCodec(Field field) {
        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        Codec codec = getValueCodecInstance(field);

        validateNotIdentityCodec(fieldName, className, codec);

        final Class<?> sourceType = codec.sourceType();
        final Class<?> targetType = codec.targetType();

        validateTypesNotNull(fieldName, className, sourceType, targetType);

        final Class<?> listValueType = TypeParser.inferValueClassForListOrSet(field.getGenericType(), field.getDeclaringClass());

        validateMatchingSourceType(fieldName, className, sourceType, listValueType);

        validateSupportedTargetType(fieldName, className, targetType);

        return ListCodecBuilder.fromType(sourceType).toType(targetType).withCodec(codec);
    }

    public SetCodec parseAndValidateSetCodec(Field field) {
        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        Codec codec = getValueCodecInstance(field);

        validateNotIdentityCodec(fieldName, className, codec);

        final Class<?> sourceType = codec.sourceType();
        final Class<?> targetType = codec.targetType();

        validateTypesNotNull(fieldName, className, sourceType, targetType);

        final Class<?> listValueType = TypeParser.inferValueClassForListOrSet(field.getGenericType(), field.getDeclaringClass());

        validateMatchingSourceType(fieldName, className, sourceType, listValueType);

        validateSupportedTargetType(fieldName, className, targetType);

        return SetCodecBuilder.fromType(sourceType).toType(targetType).withCodec(codec);
    }

    public MapCodec parseAndValidateMapCodec(Field field) {
        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        Codec keyCodec = getKeyCodecInstance(field);
        Codec valueCodec = getValueCodecInstance(field);

        Validator.validateBeanMappingFalse(keyCodec instanceof IdentityCodec && valueCodec instanceof IdentityCodec,
                "The @TypeTransformer on the field '%s' of class '%s' should declare a key/value codec other than IdentityCodec. Maybe you forgot to provided it ?",
                fieldName, className);

        final Pair<Class<Object>, Class<Object>> keyAndValueClass = TypeParser.determineMapGenericTypes(field);

        if (keyCodec instanceof IdentityCodec) {
            return buildValueMapCodec(valueCodec, field, keyAndValueClass);
        } else if (valueCodec instanceof IdentityCodec) {
            return buildKeyMapCodec(keyCodec, field, keyAndValueClass);
        } else {
            return buildKeyAndValueMapCodec(keyCodec, valueCodec, field, keyAndValueClass);
        }
    }

    private void validateNotIdentityCodec(String fieldName, String className, Codec<?, ?> codec) {
        Validator.validateBeanMappingFalse(codec instanceof IdentityCodec,
                "The @TypeTransformer on the field '%s' of class '%s' should declare a value codec other than IdentityCodec. Maybe you forgot to provided it ?", fieldName, className);
    }

    private void validateSupportedTargetType(String fieldName, String className, Class<?> targetType) {
        Validator.validateBeanMappingTrue(PropertyParser.isAssignableFromNativeType(targetType),
                "Target type '%s' declared on the field '%s' of class '%s' is not supported as primitive Cassandra data type",
                targetType.getCanonicalName(), fieldName, className);
    }

    private void validateMatchingSourceType(String fieldName, String className, Class<?> sourceType, Class<?> fieldType) {
        Validator.validateBeanMappingTrue(sourceType.isAssignableFrom(fieldType),
                "Source type '%s' of codec declared in annotation @TypeTransformer does not match Java type '%s' found on the field '%s' of class '%s'",
                sourceType.getCanonicalName(), fieldType.getCanonicalName(), fieldName, className);
    }

    private void validateTypesNotNull(String fieldName, String className, Class<?> sourceType, Class<?> targetType) {
        Validator.validateBeanMappingNotNull(sourceType,
                "Source type of codec declared in annotation @TypeTransformer on the field '%s' of class '%s' should not be null",
                fieldName, className);

        Validator.validateBeanMappingNotNull(targetType,
                "Target type of codec declared in annotation @TypeTransformer on the field '%s' of class '%s' should not be null",
                fieldName, className);
    }

    private Codec<?, ?> getValueCodecInstance(Field field) {
        final TypeTransformer typeTransformer = field.getAnnotation(TypeTransformer.class);
        final Class<?> codecClass = typeTransformer.valueCodecClass();

        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        validateInstanceOfCodec(codecClass, fieldName, className);

        return validateInstantiable(field, codecClass);
    }

    private Codec<?, ?> getKeyCodecInstance(Field field) {
        final TypeTransformer typeTransformer = field.getAnnotation(TypeTransformer.class);
        final Class<?> codecClass = typeTransformer.keyCodecClass();

        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        validateInstanceOfCodec(codecClass, fieldName, className);

        return validateInstantiable(field, codecClass);
    }

    private void validateInstanceOfCodec(Class<?> codecClass, String fieldName, String className) {
        Validator.validateBeanMappingTrue(Codec.class.isAssignableFrom(codecClass),
                "The codec class '%s' declared in @TypeTransformer on the field '%s' of class '%s' should implement the interface Codec<FROM,TO>",
                codecClass.getCanonicalName(), fieldName, className);
    }

    private Codec<?,?> validateInstantiable(Field field, Class<?> codecClass) {
        try {
            return (Codec<?,?>)codecClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AchillesBeanMappingException(format("Codec class '%s' declared on the field '%s' of class '%s' should be instantiable (declare a public constructor)",
                    codecClass.getCanonicalName(), field.getName(), field.getDeclaringClass().getCanonicalName()));
        }
    }

    private MapCodec buildKeyMapCodec(Codec keyCodec, Field field, Pair<Class<Object>, Class<Object>> keyAndValueClass) {

        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        final Class<?> sourceKeyType = keyCodec.sourceType();
        final Class<?> targetKeyType = keyCodec.targetType();

        validateTypesNotNull(fieldName, className, sourceKeyType, targetKeyType);

        validateMatchingSourceType(fieldName, className, sourceKeyType, keyAndValueClass.left);

        validateSupportedTargetType(fieldName, className, targetKeyType);

        return MapCodecBuilder
                .fromKeyType(sourceKeyType).toKeyType(targetKeyType).withKeyCodec(keyCodec)
                .withValueType(keyAndValueClass.right);
    }

    private MapCodec buildValueMapCodec(Codec valueCodec, Field field, Pair<Class<Object>, Class<Object>> keyAndValueClass) {

        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        final Class<?> sourceValueType = valueCodec.sourceType();
        final Class<?> targetValueType = valueCodec.targetType();

        validateTypesNotNull(fieldName, className, sourceValueType, targetValueType);

        validateMatchingSourceType(fieldName, className, sourceValueType, keyAndValueClass.right);

        validateSupportedTargetType(fieldName, className, targetValueType);

        return MapCodecBuilder
                .withKeyType(keyAndValueClass.left)
                .fromValueType(sourceValueType).toValueType(targetValueType).withValueCodec(valueCodec);
    }

    private MapCodec buildKeyAndValueMapCodec(Codec keyCodec, Codec valueCodec, Field field, Pair<Class<Object>, Class<Object>> keyAndValueClass) {

        final String fieldName = field.getName();
        final String className = field.getDeclaringClass().getCanonicalName();

        final Class<?> sourceKeyType = keyCodec.sourceType();
        final Class<?> targetKeyType = keyCodec.targetType();

        final Class<?> sourceValueType = valueCodec.sourceType();
        final Class<?> targetValueType = valueCodec.targetType();

        validateTypesNotNull(fieldName, className, sourceKeyType, targetKeyType);
        validateTypesNotNull(fieldName, className, sourceValueType, targetValueType);


        validateMatchingSourceType(fieldName, className, sourceKeyType, keyAndValueClass.left);
        validateMatchingSourceType(fieldName, className, sourceValueType, keyAndValueClass.right);

        validateSupportedTargetType(fieldName, className, targetKeyType);
        validateSupportedTargetType(fieldName, className, targetValueType);

        return MapCodecBuilder
                .fromKeyType(sourceKeyType).toKeyType(targetKeyType).withKeyCodec(keyCodec)
                .fromValueType(sourceValueType).toValueType(targetValueType).withValueCodec(valueCodec);
    }

    public static enum Singleton {
        INSTANCE;

        private final TypeTransformerParser instance = new TypeTransformerParser();

        public TypeTransformerParser get() {
            return instance;
        }
    }
}
