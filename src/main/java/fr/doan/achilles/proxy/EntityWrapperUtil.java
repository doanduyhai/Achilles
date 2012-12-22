package fr.doan.achilles.proxy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import net.sf.cglib.proxy.Factory;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.holder.KeyValue;
import fr.doan.achilles.proxy.interceptor.AchillesInterceptor;

public class EntityWrapperUtil
{
	public boolean isProxy(Object entity)
	{
		return Factory.class.isAssignableFrom(entity.getClass());
	}

	@SuppressWarnings("rawtypes")
	public Class deriveBaseClass(Object entity)
	{
		Class baseClass = entity.getClass();
		if (isProxy(entity))
		{
			Factory proxy = (Factory) entity;
			AchillesInterceptor interceptor = (AchillesInterceptor) proxy.getCallback(0);
			baseClass = interceptor.getTarget().getClass();
		}

		return baseClass;
	}

	public Object determinePrimaryKey(Object entity, EntityMeta<?> entityMeta)
	{
		Object key;
		try
		{
			key = entityMeta.getIdMeta().getGetter().invoke(entity);
		}
		catch (Exception e)
		{
			key = null;
		}
		return key;
	}

	public List<Object> determineMultiKey(Object entity, List<Method> componentGetters)
	{
		List<Object> multiKeyValues = new ArrayList<Object>();

		for (Method getter : componentGetters)
		{
			Object key = null;
			try
			{
				key = getter.invoke(entity);
			}
			catch (Exception e)
			{
				// TODO, log error
			}
			multiKeyValues.add(key);
		}

		return multiKeyValues;
	}

	public <K, V> List<KeyValue<K, V>> buildMultiKeyListForComposite(
			PropertyMeta<K, V> wideMapMeta, List<HColumn<Composite, Object>> hColumns)
	{
		List<KeyValue<K, V>> results = new ArrayList<KeyValue<K, V>>();

		for (HColumn<Composite, Object> column : hColumns)
		{
			results.add(buildMultiKeyForComposite(wideMapMeta, column));
		}
		return results;
	}

	public <K, V> KeyValue<K, V> buildMultiKeyForComposite(PropertyMeta<K, V> wideMapMeta,
			HColumn<Composite, Object> column)
	{
		Class<K> multiKeyClass = wideMapMeta.getKeyClass();
		List<Method> componentSetters = wideMapMeta.getComponentSetters();

		List<Serializer<?>> serializers = wideMapMeta.getComponentSerializers();
		KeyValue<K, V> result = null;
		try
		{
			K multiKeyInstance = multiKeyClass.newInstance();
			List<Component<?>> components = column.getName().getComponents();

			for (int i = 0; i < components.size(); i++)
			{
				Component<?> comp = components.get(i);
				Object compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
				componentSetters.get(i).invoke(multiKeyInstance, compValue);
			}

			V value = wideMapMeta.getValue(column.getValue());
			result = new KeyValue<K, V>(multiKeyInstance, value, column.getTtl());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return result;
	}
}
