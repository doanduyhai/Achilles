package info.archinnov.achilles.compound;

import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import com.google.common.base.Function;

public class ClassToSerializerTransformer implements Function<Class<?>, Serializer<Object>> {

    @Override
    public Serializer<Object> apply(Class<?> clazz) {
        return ThriftSerializerTypeInferer.getSerializer(clazz);
    }

}
