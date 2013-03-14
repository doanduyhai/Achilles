package testBuilders;

import static info.archinnov.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * HColumTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unchecked")
public class HColumnTestBuilder
{

	public static <V> HColumn<Composite, V> simple(Composite name, V value)
	{
		return HFactory.createColumn(name, value, COMPOSITE_SRZ,
				(Serializer<V>) SerializerTypeInferer.getSerializer(value));
	}

	public static <V> HColumn<Composite, V> simple(Composite name, V value, int ttl)
	{
		return HFactory.createColumn(name, value, ttl, COMPOSITE_SRZ,
				(Serializer<V>) SerializerTypeInferer.getSerializer(value));
	}

	public static HColumn<DynamicComposite, String> dynamic(DynamicComposite name, String value)
	{
		return HFactory.createColumn(name, value, DYNA_COMP_SRZ, STRING_SRZ);
	}

	public static HColumn<DynamicComposite, String> dynamic(DynamicComposite name, String value,
			int ttl)
	{
		return HFactory.createColumn(name, value, ttl, DYNA_COMP_SRZ, STRING_SRZ);
	}

	public static HCounterColumn<DynamicComposite> counter(DynamicComposite name, Long value)
	{
		return HFactory.createCounterColumn(name, value, DYNA_COMP_SRZ);
	}
}
