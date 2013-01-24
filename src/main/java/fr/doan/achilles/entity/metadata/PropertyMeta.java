package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.Serializer;

/**
 * PropertyMeta<K, V>
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMeta<K, V>
{
	private PropertyType type;
	private String propertyName;
	private Class<K> keyClass;
	private Serializer<K> keySerializer;
	private Class<V> valueClass;
	private Serializer<V> valueSerializer;
	private Method getter;
	private Method setter;

	private JoinProperties joinProperties;
	private MultiKeyProperties multiKeyProperties;
	private ExternalWideMapProperties<?> externalWideMapProperties;

	private boolean singleKey;

	public PropertyType type()
	{
		return type;
	}

	public void setType(PropertyType propertyType)
	{
		this.type = propertyType;
	}

	public List<V> newListInstance()
	{
		return new ArrayList<V>();
	}

	public Set<V> newSetInstance()
	{
		return new HashSet<V>();
	}

	public Map<K, V> newMapInstance()
	{
		return new HashMap<K, V>();
	}

	public String getPropertyName()
	{
		return propertyName;
	}

	public void setPropertyName(String propertyName)
	{
		this.propertyName = propertyName;
	}

	public Class<K> getKeyClass()
	{
		return keyClass;
	}

	public void setKeyClass(Class<K> keyClass)
	{
		this.keyClass = keyClass;
	}

	public Serializer<K> getKeySerializer()
	{
		return keySerializer;
	}

	public void setKeySerializer(Serializer<K> keySerializer)
	{
		this.keySerializer = keySerializer;
	}

	public Class<V> getValueClass()
	{
		return valueClass;
	}

	public void setValueClass(Class<V> valueClass)
	{
		this.valueClass = valueClass;
	}

	public Serializer<V> getValueSerializer()
	{
		return valueSerializer;
	}

	public void setValueSerializer(Serializer<V> valueSerializer)
	{
		this.valueSerializer = valueSerializer;
	}

	public Method getGetter()
	{
		return getter;
	}

	public void setGetter(Method getter)
	{
		this.getter = getter;
	}

	public Method getSetter()
	{
		return setter;
	}

	public void setSetter(Method setter)
	{
		this.setter = setter;
	}

	public MultiKeyProperties getMultiKeyProperties()
	{
		return multiKeyProperties;
	}

	public void setMultiKeyProperties(MultiKeyProperties multiKeyProperties)
	{
		this.multiKeyProperties = multiKeyProperties;
	}

	public K getKey(Object object)
	{
		return keyClass.cast(object);
	}

	public V getValue(Object object)
	{
		try
		{
			return this.valueClass.cast(object);

		}
		catch (ClassCastException e)
		{
			return null;
		}
	}

	public JoinProperties getJoinProperties()
	{
		return joinProperties;
	}

	public void setJoinProperties(JoinProperties joinProperties)
	{
		this.joinProperties = joinProperties;
	}

	public ExternalWideMapProperties<?> getExternalWideMapProperties()
	{
		return externalWideMapProperties;
	}

	public void setExternalWideMapProperties(ExternalWideMapProperties<?> externalWideMapProperties)
	{
		this.externalWideMapProperties = externalWideMapProperties;
	}

	public boolean isSingleKey()
	{
		return singleKey;
	}

	public void setSingleKey(boolean singleKey)
	{
		this.singleKey = singleKey;
	}
}
