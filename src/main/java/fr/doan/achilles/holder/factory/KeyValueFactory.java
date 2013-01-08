package fr.doan.achilles.holder.factory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.MultiKeyProperties;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.EntityLoader;
import fr.doan.achilles.holder.KeyValue;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	private EntityLoader loader = new EntityLoader();
	private EntityHelper helper = new EntityHelper();

	public <K, V> KeyValue<K, V> create(K key, V value, int ttl)
	{
		return new KeyValue<K, V>(key, value, ttl);
	}

	public <K, V> KeyValue<K, V> create(K key, V value)
	{
		return new KeyValue<K, V>(key, value);
	}

	public <K, V> KeyValue<K, V> createForWideMap(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		K key;
		V value;
		int ttl;

		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(2, propertyMeta.getKeySerializer());
			value = extractValueFromDynamicCompositeHColumn(propertyMeta, hColumn);
			ttl = hColumn.getTtl();
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			Class<K> multiKeyClass = propertyMeta.getKeyClass();
			List<Method> componentSetters = multiKeyProperties.getComponentSetters();
			List<Serializer<?>> serializers = multiKeyProperties.getComponentSerializers();
			try
			{
				key = multiKeyClass.newInstance();
				List<Component<?>> components = hColumn.getName().getComponents();

				for (int i = 2; i < components.size(); i++)
				{
					Component<?> comp = components.get(i);
					Object compValue = serializers.get(i - 2).fromByteBuffer(comp.getBytes());
					helper.setValueToField(key, componentSetters.get(i - 2), compValue);
				}

				value = extractValueFromDynamicCompositeHColumn(propertyMeta, hColumn);
				ttl = hColumn.getTtl();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				throw new RuntimeException(e.getMessage(), e);
			}
		}

		return create(key, value, ttl);
	}

	@SuppressWarnings("unchecked")
	private <K, V> V extractValueFromDynamicCompositeHColumn(PropertyMeta<K, V> propertyMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		V value = null;
		Object hColumnValue = hColumn.getValue();
		if (propertyMeta.isJoinColumn())
		{

			value = (V) loader.loadJoinEntity(propertyMeta.getValueClass(), hColumnValue,
					propertyMeta.getJoinProperties().getEntityMeta());
		}
		else
		{
			value = propertyMeta.getValue(hColumnValue);
		}
		return value;
	}

	public <K, V> List<KeyValue<K, V>> createListForWideMap(PropertyMeta<K, V> propertyMeta,
			List<HColumn<DynamicComposite, Object>> hColumns)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<DynamicComposite, Object> hColumn : hColumns)
			{
				result.add(createForWideMap(propertyMeta, hColumn));
			}
		}
		return result;
	}

	public <K, V> KeyValue<K, V> createForWideRowOrExternalWideMapMeta(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, ?> hColumn)
	{
		K key;
		V value;
		int ttl;

		if (propertyMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(0, propertyMeta.getKeySerializer());
			value = extractValueFromCompositeHColumn(propertyMeta, hColumn);
			ttl = hColumn.getTtl();
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			Class<K> multiKeyClass = propertyMeta.getKeyClass();
			List<Method> componentSetters = multiKeyProperties.getComponentSetters();
			List<Serializer<?>> serializers = multiKeyProperties.getComponentSerializers();
			try
			{
				key = multiKeyClass.newInstance();
				List<Component<?>> components = hColumn.getName().getComponents();

				for (int i = 0; i < components.size(); i++)
				{
					Component<?> comp = components.get(i);
					Object compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
					helper.setValueToField(key, componentSetters.get(i), compValue);
				}

				value = extractValueFromCompositeHColumn(propertyMeta, hColumn);
				ttl = hColumn.getTtl();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				throw new RuntimeException(e.getMessage(), e);
			}
		}

		return create(key, value, ttl);
	}

	@SuppressWarnings("unchecked")
	private <K, V, W> V extractValueFromCompositeHColumn(PropertyMeta<K, V> propertyMeta,
			HColumn<Composite, W> hColumn)
	{
		V value = null;
		W hColumnValue = hColumn.getValue();
		if (propertyMeta.isJoinColumn())
		{

			value = (V) loader.loadJoinEntity(propertyMeta.getValueClass(), hColumnValue,
					propertyMeta.getJoinProperties().getEntityMeta());
		}
		else
		{
			value = propertyMeta.getValue(hColumnValue);
		}
		return value;
	}

	public <K, V> List<KeyValue<K, V>> createListForWideRowOrExternalWideMapMeta(PropertyMeta<K, V> propertyMeta,
			List<HColumn<Composite, ?>> hColumns)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<Composite, ?> hColumn : hColumns)
			{
				result.add(createForWideRowOrExternalWideMapMeta(propertyMeta, hColumn));
			}
		}
		return result;
	}

}
