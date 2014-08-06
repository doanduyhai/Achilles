package info.archinnov.achilles.internal.metadata.transcoding.codec;

public class SetCodecBuilder {

    public static <VAL> SetCodec<VAL, VAL> withType(Class<VAL> valueType) {
        return new SetCodecImpl<>(valueType, valueType, new NativeCodec<>(valueType));
    }

    public static <FROM> FromType<FROM> fromType(Class<FROM> fromType) {
        return new FromType<>(fromType);
    }

    public static class FromType<FROM> {
        private final Class<FROM> fromType;

        private FromType(Class<FROM> fromType) {
            this.fromType = fromType;
        }

        public <TO> WithCodec<FROM, TO> toType(Class<TO> toType) {
            return new WithCodec<>(fromType, toType);
        }
    }

    public static class WithCodec<FROM, TO> {
        private final Class<FROM> fromType;
        private final Class<TO> toType;

        private WithCodec(Class<FROM> fromType, Class<TO> toType) {
            this.fromType = fromType;
            this.toType = toType;
        }

        public SetCodec<FROM, TO> withCodec(SimpleCodec<FROM, TO> codec) {
            return new SetCodecImpl<>(fromType, toType, codec);
        }
    }
}
