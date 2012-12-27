package fr.doan.achilles.holder.factory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.operations.JoinEntityLoader;
import fr.doan.achilles.holder.KeyValue;

/**
 * KeyValueFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class KeyValueFactory
{
	private JoinEntityLoader joinEntityLoader = new JoinEntityLoader();

	public <K, V> KeyValue<K, V> create(K key, V value, int ttl)
	{
		return new KeyValue<K, V>(key, value, ttl);
	}

	public <K, V> KeyValue<K, V> create(K key, V value)
	{
		return new KeyValue<K, V>(key, value);
	}

	@SuppressWarnings("unchecked")
	public <K, V> KeyValue<K, V> createForWideMap(PropertyMeta<K, V> wideMapMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		K key;
		V value;
		int ttl;

		if (wideMapMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(2, wideMapMeta.getKeySerializer());
			value = extractValueFromHColumn(wideMapMeta, hColumn);
			ttl = hColumn.getTtl();
		}
		else
		{
			Class<K> multiKeyClass = wideMapMeta.getKeyClass();
			List<Method> componentSetters = wideMapMeta.getComponentSetters();
			List<Serializer<?>> serializers = wideMapMeta.getComponentSerializers();
			try
			{
				key = multiKeyClass.newInstance();
				List<Component<?>> components = hColumn.getName().getComponents();

				for (int i = 2; i < components.size(); i++)
				{
					Component<?> comp = components.get(i);
					Object compValue = serializers.get(i - 2).fromByteBuffer(comp.getBytes());
					componentSetters.get(i - 2).invoke(key, compValue);
				}

				value = extractValueFromHColumn(wideMapMeta, hColumn);
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

	private <K, V> V extractValueFromHColumn(PropertyMeta<K, V> wideMapMeta,
			HColumn<DynamicComposite, Object> hColumn)
	{
		V value = null;
		Object hColumnValue = hColumn.getValue();
		if (wideMapMeta.isJoinColumn())
		{
			if (hColumnValue != null)
			{
				value = joinEntityLoader.loadJoinEntity(hColumnValue, wideMapMeta);
			}
		}
		else
		{
			value = wideMapMeta.getValue(hColumnValue);
		}
		return value;
	}

	public <K, V> List<KeyValue<K, V>> createListForWideMap(PropertyMeta<K, V> wideMapMeta,
			List<HColumn<DynamicComposite, Object>> hColumns)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<DynamicComposite, Object> hColumn : hColumns)
			{
				result.add(createForWideMap(wideMapMeta, hColumn));
			}
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public <K, V> KeyValue<K, V> createForWideRow(PropertyMeta<K, V> wideMapMeta,
			HColumn<Composite, V> hColumn)
	{
		K key;
		V value;
		int ttl;

		if (wideMapMeta.isSingleKey())
		{
			key = (K) hColumn.getName().get(0, wideMapMeta.getKeySerializer());
			value = wideMapMeta.getValue(hColumn.getValue());
			ttl = hColumn.getTtl();
		}
		else
		{
			Class<K> multiKeyClass = wideMapMeta.getKeyClass();
			List<Method> componentSetters = wideMapMeta.getComponentSetters();
			List<Serializer<?>> serializers = wideMapMeta.getComponentSerializers();
			try
			{
				key = multiKeyClass.newInstance();
				List<Component<?>> components = hColumn.getName().getComponents();

				for (int i = 0; i < components.size(); i++)
				{
					Component<?> comp = components.get(i);
					Object compValue = serializers.get(i).fromByteBuffer(comp.getBytes());
					componentSetters.get(i).invoke(key, compValue);
				}

				value = wideMapMeta.getValue(hColumn.getValue());
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

	public <K, V> List<KeyValue<K, V>> createListForWideRow(PropertyMeta<K, V> wideMapMeta,
			List<HColumn<Composite, V>> hColumns)
	{
		List<KeyValue<K, V>> result = new ArrayList<KeyValue<K, V>>();
		if (hColumns != null && hColumns.size() > 0)
		{
			for (HColumn<Composite, V> hColumn : hColumns)
			{
				result.add(createForWideRow(wideMapMeta, hColumn));
			}
		}
		return result;
	}
}
