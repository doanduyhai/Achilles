package info.archinnov.achilles.internal.metadata.codec;

public class ListCodecBuilder {

    public static <VAL> ListCodec<VAL, VAL> withType(Class<VAL> valueType) {
        return new ListCodecImpl<>(valueType, valueType, new NativeCodec<>(valueType));
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

        public ListCodec<FROM, TO> withCodec(SimpleCodec<FROM, TO> codec) {
            return new ListCodecImpl<>(fromType, toType, codec);
        }
    }
}
