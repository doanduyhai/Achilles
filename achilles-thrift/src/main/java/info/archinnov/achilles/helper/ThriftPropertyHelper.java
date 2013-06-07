package info.archinnov.achilles.helper;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.MethodInvoker;
import info.archinnov.achilles.type.MultiKey;
import info.archinnov.achilles.type.WideMap;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPropertyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPropertyHelper extends PropertyHelper
{
	private static final Logger log = LoggerFactory.getLogger(ThriftPropertyHelper.class);

	private MethodInvoker invoker = new MethodInvoker();

	public <ID> String determineCompatatorTypeAliasForCompositeCF(PropertyMeta<?, ?> propertyMeta,
			boolean forCreation)
	{

		log
				.debug("Determine the Comparator type alias for composite-based column family using propertyMeta of field {} ",
						propertyMeta.getPropertyName());

		Class<?> nameClass = propertyMeta.getKeyClass();
		List<String> comparatorTypes = new ArrayList<String>();
		String comparatorTypesAlias;

		if (MultiKey.class.isAssignableFrom(nameClass))
		{

			MultiKeyProperties multiKeyProperties = this.parseMultiKey(nameClass);

			for (Class<?> clazz : multiKeyProperties.getComponentClasses())
			{
				Serializer<?> srz = SerializerTypeInferer.getSerializer(clazz);
				if (forCreation)
				{
					comparatorTypes.add(srz.getComparatorType().getTypeName());
				}
				else
				{
					comparatorTypes.add("org.apache.cassandra.db.marshal."
							+ srz.getComparatorType().getTypeName());
				}
			}
			if (forCreation)
			{
				comparatorTypesAlias = "(" + StringUtils.join(comparatorTypes, ',') + ")";
			}
			else
			{
				comparatorTypesAlias = "CompositeType(" + StringUtils.join(comparatorTypes, ',')
						+ ")";
			}
		}
		else
		{
			String typeAlias = SerializerTypeInferer
					.getSerializer(nameClass)
					.getComparatorType()
					.getTypeName();
			if (forCreation)
			{
				comparatorTypesAlias = "(" + typeAlias + ")";
			}
			else
			{
				comparatorTypesAlias = "CompositeType(org.apache.cassandra.db.marshal." + typeAlias
						+ ")";
			}
		}

		log.trace("Comparator type alias : {}", comparatorTypesAlias);

		return comparatorTypesAlias;
	}

	public <K, V> K buildMultiKeyFromComposite(PropertyMeta<K, V> propertyMeta,
			List<Component<?>> components)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Build multi-key instance of field {} from composite components {}",
					propertyMeta.getPropertyName(), format(components));
		}

		K key;
		MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
		Class<K> multiKeyClass = propertyMeta.getKeyClass();
		List<Method> componentSetters = multiKeyProperties.getComponentSetters();
		List<Serializer<?>> serializers = new ArrayList<Serializer<?>>();
		for (Class<?> clazz : multiKeyProperties.getComponentClasses())
		{
			serializers.add(SerializerTypeInferer.getSerializer(clazz));
		}

		try
		{
			key = multiKeyClass.newInstance();

			for (int i = 0; i < components.size(); i++)
			{
				Component<?> comp = components.get(i);
				Object compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
				invoker.setValueToField(key, componentSetters.get(i), compValue);
			}

		}
		catch (Exception e)
		{
			throw new AchillesException(e);
		}

		log.trace("Built multi key : {}", key);

		return key;
	}

	public ComponentEquality[] determineEquality(WideMap.BoundingMode bounds,
			WideMap.OrderingMode ordering)
	{
		log
				.trace("Determine component equality with respect to bounding mode {} and ordering mode {}",
						bounds.name(), ordering.name());
		ComponentEquality[] result = new ComponentEquality[2];
		if (WideMap.OrderingMode.DESCENDING.equals(ordering))
		{
			if (WideMap.BoundingMode.INCLUSIVE_BOUNDS.equals(bounds))
			{
				result[0] = GREATER_THAN_EQUAL;
				result[1] = EQUAL;
			}
			else if (WideMap.BoundingMode.EXCLUSIVE_BOUNDS.equals(bounds))
			{
				result[0] = LESS_THAN_EQUAL;
				result[1] = GREATER_THAN_EQUAL;
			}
			else if (WideMap.BoundingMode.INCLUSIVE_START_BOUND_ONLY.equals(bounds))
			{
				result[0] = GREATER_THAN_EQUAL;
				result[1] = GREATER_THAN_EQUAL;
			}
			else if (WideMap.BoundingMode.INCLUSIVE_END_BOUND_ONLY.equals(bounds))
			{
				result[0] = LESS_THAN_EQUAL;
				result[1] = EQUAL;
			}
		}
		else
		{
			if (WideMap.BoundingMode.INCLUSIVE_BOUNDS.equals(bounds))
			{
				result[0] = EQUAL;
				result[1] = GREATER_THAN_EQUAL;
			}
			else if (WideMap.BoundingMode.EXCLUSIVE_BOUNDS.equals(bounds))
			{
				result[0] = GREATER_THAN_EQUAL;
				result[1] = LESS_THAN_EQUAL;
			}
			else if (WideMap.BoundingMode.INCLUSIVE_START_BOUND_ONLY.equals(bounds))
			{
				result[0] = EQUAL;
				result[1] = LESS_THAN_EQUAL;
			}
			else if (WideMap.BoundingMode.INCLUSIVE_END_BOUND_ONLY.equals(bounds))
			{
				result[0] = GREATER_THAN_EQUAL;
				result[1] = GREATER_THAN_EQUAL;
			}
		}

		log
				.trace("For the to bounding mode {} and ordering mode {}, the component equalities should be : {} - {}",
						bounds.name(), ordering.name(), result[0].name(), result[1].name());
		return result;
	}
}
