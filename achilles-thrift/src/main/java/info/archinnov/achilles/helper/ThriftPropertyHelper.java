package info.archinnov.achilles.helper;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;
import info.archinnov.achilles.type.MultiKey;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftPropertyHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftPropertyHelper extends AchillesPropertyHelper
{
	private static final Logger log = LoggerFactory.getLogger(ThriftPropertyHelper.class);

	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();

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
}
