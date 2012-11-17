package fr.doan.achilles.serializer;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;

public class Utils
{
	public static final StringSerializer STRING_SRZ = StringSerializer.get();
	public static final LongSerializer LONG_SRZ = LongSerializer.get();
	public static final IntegerSerializer INT_SRZ = IntegerSerializer.get();
	public static final UUIDSerializer UUID_SRZ = UUIDSerializer.get();
	public static final TimeUUIDSerializer TIMEUUID_SRZ = TimeUUIDSerializer.get();
	public static final CompositeSerializer COMPOSITE_SRZ = CompositeSerializer.get();
	public static final DynamicCompositeSerializer DYNA_COMP_SRZ = DynamicCompositeSerializer.get();
	public static final DateSerializer DATE_SRZ = DateSerializer.get();
	public static final DoubleSerializer DOUBLE_SRZ = DoubleSerializer.get();
	public static final ObjectSerializer OBJECT_SRZ = ObjectSerializer.get();
	public static final BytesArraySerializer BYTE_SRZ = BytesArraySerializer.get();
}
