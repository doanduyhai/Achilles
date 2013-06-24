package info.archinnov.achilles.serializer;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static me.prettyprint.hector.api.ddl.ComparatorType.UTF8TYPE;
import java.nio.ByteBuffer;
import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

public class ThriftEnumSerializer<E extends Enum<E>> extends AbstractSerializer<Enum<E>> {

    private Class<E> type;

    public ThriftEnumSerializer(Class<E> type) {
        this.type = type;
    }

    @Override
    public ByteBuffer toByteBuffer(Enum<E> enumInstance) {
        if (enumInstance == null) {
            return null;
        }
        String enumValue = enumInstance.ordinal() + "=" + enumInstance.name();
        return STRING_SRZ.toByteBuffer(enumValue);
    }

    @Override
    public Enum<E> fromByteBuffer(ByteBuffer byteBuffer) {

        String enumValue = STRING_SRZ.fromByteBuffer(byteBuffer);
        String[] enumSplit = enumValue.split("=");
        return Enum.valueOf(type, enumSplit[1]);
    }

    @Override
    public ComparatorType getComparatorType() {
        return UTF8TYPE;
    }
}
