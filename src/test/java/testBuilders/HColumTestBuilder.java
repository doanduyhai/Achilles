package testBuilders;

import static fr.doan.achilles.serializer.SerializerUtils.COMPOSITE_SRZ;
import static fr.doan.achilles.serializer.SerializerUtils.DYNA_COMP_SRZ;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * HColumTestBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unchecked")
public class HColumTestBuilder
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

	public static <V> HColumn<DynamicComposite, Object> dynamic(DynamicComposite name, V value)
	{
		return HFactory.createColumn(name, value, DYNA_COMP_SRZ,
				(Serializer<Object>) SerializerTypeInferer.getSerializer(value));
	}

	public static <V> HColumn<DynamicComposite, Object> dynamic(DynamicComposite name, V value,
			int ttl)
	{
		return HFactory.createColumn(name, value, ttl, DYNA_COMP_SRZ,
				(Serializer<Object>) SerializerTypeInferer.getSerializer(value));
	}

}
