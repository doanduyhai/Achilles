package info.archinnov.achilles.internal.metadata.codec;

import info.archinnov.achilles.codec.Codec;

public class MapCodecBuilder {

    public static <TYPE> FromSourceKeyType<TYPE> fromKeyType(Class<TYPE> sourceKeyType) {
        return new FromSourceKeyType<>(sourceKeyType);
    }

    public static <TYPE> WithKeyType<TYPE> withKeyType(Class<TYPE> keyType) {
        return new WithKeyType<>(keyType);
    }

    public static class FromSourceKeyType<FROM_KEY> {
        private final Class<FROM_KEY> sourceKeyType;

        private FromSourceKeyType(Class<FROM_KEY> sourceKeyType) {
            this.sourceKeyType = sourceKeyType;
        }

        public <TO_KEY> ToTargetKeyType<FROM_KEY, TO_KEY> toKeyType(Class<TO_KEY> targetKeyType) {
            return new ToTargetKeyType<>(sourceKeyType, targetKeyType);
        }
    }

    public static class ToTargetKeyType<FROM_KEY, TO_KEY> {
        private final Class<FROM_KEY> sourceKeyType;
        private final Class<TO_KEY> targetKeyType;

        private ToTargetKeyType(Class<FROM_KEY> sourceKeyType, Class<TO_KEY> targetKeyType) {
            this.sourceKeyType = sourceKeyType;
            this.targetKeyType = targetKeyType;
        }

        public WithKeyCodec<FROM_KEY, TO_KEY> withKeyCodec(Codec<FROM_KEY, TO_KEY> keyCodec) {
            return new WithKeyCodec<>(sourceKeyType, targetKeyType, keyCodec);
        }
    }

    public static class WithKeyType<KEY> {

        private final Class<KEY> keyType;

        private WithKeyType(Class<KEY> keyType) {
            this.keyType = keyType;
        }

        public <FROM_VAL> FromSourceValueType<KEY, KEY, FROM_VAL> fromValueType(Class<FROM_VAL> sourceValueType) {
            return new FromSourceValueType<>(keyType, keyType, new NativeCodec<>(keyType), sourceValueType);
        }

        public <VAL> MapCodec<KEY, VAL, KEY, VAL> withValueType(Class<VAL> valueType) {
            return new MapCodecImpl<>(keyType, valueType, keyType, valueType, new NativeCodec<>(keyType), new NativeCodec<>(valueType));
        }
    }

    public static class WithKeyCodec<FROM_KEY, TO_KEY> {
        private final Class<FROM_KEY> sourceKeyType;
        private final Class<TO_KEY> targetKeyType;
        private final Codec<FROM_KEY, TO_KEY> sourceCodec;

        private WithKeyCodec(Class<FROM_KEY> sourceKeyType, Class<TO_KEY> targetKeyType, Codec<FROM_KEY, TO_KEY> sourceCodec) {
            this.sourceKeyType = sourceKeyType;
            this.targetKeyType = targetKeyType;
            this.sourceCodec = sourceCodec;
        }

        public <FROM_VAL> FromSourceValueType<FROM_KEY, TO_KEY, FROM_VAL> fromValueType(Class<FROM_VAL> sourceValueType) {
            return new FromSourceValueType<>(sourceKeyType, targetKeyType, sourceCodec, sourceValueType);
        }

        public <VAL> MapCodec<FROM_KEY, VAL, TO_KEY, VAL> withValueType(Class<VAL> valueType) {
            return new MapCodecImpl<>(sourceKeyType, valueType, targetKeyType, valueType, sourceCodec, new NativeCodec<>(valueType));
        }

    }

    /*************** VALUE ***********************/

    public static class FromSourceValueType<FROM_KEY, TO_KEY, FROM_VAL> {
        private final Class<FROM_KEY> sourceKeyType;
        private final Class<TO_KEY> targetKeyType;
        private final Codec<FROM_KEY, TO_KEY> keyCodec;
        private final Class<FROM_VAL> sourceValueType;

        private FromSourceValueType(Class<FROM_KEY> sourceKeyType, Class<TO_KEY> targetKeyType, Codec<FROM_KEY, TO_KEY> keyCodec, Class<FROM_VAL> sourceValueType) {
            this.sourceKeyType = sourceKeyType;
            this.targetKeyType = targetKeyType;
            this.keyCodec = keyCodec;
            this.sourceValueType = sourceValueType;
        }

        public <TO_VAL> ToTargetValueType<FROM_KEY, TO_KEY, FROM_VAL, TO_VAL> toValueType(Class<TO_VAL> targetValueType) {
            return new ToTargetValueType<>(sourceKeyType, targetKeyType, keyCodec, sourceValueType, targetValueType);
        }
    }

    public static class ToTargetValueType<FROM_KEY, TO_KEY, FROM_VAL, TO_VAL> {
        private final Class<FROM_KEY> sourceKeyType;
        private final Class<TO_KEY> targetKeyType;
        private final Codec<FROM_KEY, TO_KEY> keyCodec;
        private final Class<FROM_VAL> sourceValueType;
        private final Class<TO_VAL> targetValueType;

        private ToTargetValueType(Class<FROM_KEY> sourceKeyType, Class<TO_KEY> targetKeyType, Codec<FROM_KEY, TO_KEY> keyCodec, Class<FROM_VAL> sourceValueType, Class<TO_VAL> targetValueType) {
            this.sourceKeyType = sourceKeyType;
            this.targetKeyType = targetKeyType;
            this.keyCodec = keyCodec;
            this.sourceValueType = sourceValueType;
            this.targetValueType = targetValueType;
        }

        public MapCodec<FROM_KEY, FROM_VAL, TO_KEY, TO_VAL> withValueCodec(Codec<FROM_VAL, TO_VAL> valueCodec) {
            return new MapCodecImpl<>(sourceKeyType, sourceValueType, targetKeyType, targetValueType, keyCodec, valueCodec);
        }
    }

}
