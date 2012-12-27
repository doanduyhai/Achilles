package fr.doan.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;

public abstract class PropertyMeta<K, V>
{

	private String propertyName;
	private Class<K> keyClass;
	private Serializer<?> keySerializer;
	private Class<V> valueClass;
	private Serializer<?> valueSerializer;
	private Method getter;
	private Method setter;
	private boolean lazy = false;
	private boolean singleKey = true;
	private boolean joinColumn = false;

	private JoinMetaHolder<?> joinMetaHolder;
	private List<Class<?>> componentClasses;
	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	public abstract PropertyType propertyType();

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

	public Serializer<?> getKeySerializer()
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

	public Serializer<?> getValueSerializer()
	{
		return valueSerializer;
	}

	public void setValueSerializer(Serializer<?> valueSerializer)
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

	public boolean isLazy()
	{
		return lazy;
	}

	public void setLazy(boolean lazy)
	{
		this.lazy = lazy;
	}

	public boolean isSingleKey()
	{
		return singleKey;
	}

	public void setSingleKey(boolean singleKey)
	{
		this.singleKey = singleKey;
	}

	public boolean isJoinColumn()
	{
		return joinColumn;
	}

	public void setJoinColumn(boolean joinColumn)
	{
		this.joinColumn = joinColumn;
	}

	public List<Class<?>> getComponentClasses()
	{
		return componentClasses;
	}

	public void setComponentClasses(List<Class<?>> componentClasses)
	{
		this.componentClasses = componentClasses;
	}

	public List<Serializer<?>> getComponentSerializers()
	{
		return componentSerializers;
	}

	public void setComponentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
	}

	public List<Method> getComponentGetters()
	{
		return componentGetters;
	}

	public void setComponentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
	}

	public List<Method> getComponentSetters()
	{
		return componentSetters;
	}

	public void setComponentSetters(List<Method> componentSetters)
	{
		this.componentSetters = componentSetters;
	}

	public K getKey(Object object)
	{
		return keyClass.cast(object);
	}

	public V getValue(Object object)
	{
		return this.valueClass.cast(object);
	}

	public JoinMetaHolder<?> getJoinMetaHolder()
	{
		return joinMetaHolder;
	}

	public void setJoinMetaHolder(JoinMetaHolder<?> joinMetaHolder)
	{
		this.joinMetaHolder = joinMetaHolder;
	}
}
