package info.archinnov.achilles.serializer;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;

public class ThriftSerializerTypeInferer {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Serializer<T> getSerializer(Class<?> valueClass) {
        if (valueClass == null)
        {
            return null;
        }

        //        if (valueClass.isEnum())
        //        {
        //            return new ThriftEnumSerializer(valueClass);
        //        }

        Serializer<T> serializer = SerializerTypeInferer.getSerializer(valueClass);

        if (serializer == null || serializer.equals(ThriftSerializerUtils.OBJECT_SRZ))
        {
            return SerializerTypeInferer.getSerializer(String.class);
        }
        else
        {
            return serializer;
        }
    }

    public static <T> Serializer<T> getSerializer(Object value) {
        if (value == null)
        {
            return null;
        }

        Class<?> valueClass = value.getClass();

        return getSerializer(valueClass);
    }
}
