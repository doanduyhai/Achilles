package info.archinnov.achilles.serializer;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;

public class ThriftSerializerTypeInferer {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Serializer<T> getSerializer(Class<?> valueClass) {
        Serializer srz = SerializerTypeInferer.getSerializer(valueClass);
        if (srz == null || srz == ThriftSerializerUtils.OBJECT_SRZ)
        {
            srz = ThriftSerializerUtils.STRING_SRZ;
        }
        return srz;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Serializer<T> getSerializer(Object value) {
        Serializer srz = SerializerTypeInferer.getSerializer(value);
        if (srz == null || srz == ThriftSerializerUtils.OBJECT_SRZ)
        {
            srz = ThriftSerializerUtils.STRING_SRZ;
        }
        return srz;
    }
}
