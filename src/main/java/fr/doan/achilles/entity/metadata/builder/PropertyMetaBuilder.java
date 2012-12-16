package fr.doan.achilles.entity.metadata.builder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.entity.metadata.ListLazyMeta;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapLazyMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.entity.metadata.SetLazyMeta;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.entity.metadata.SimpleLazyMeta;
import fr.doan.achilles.entity.metadata.SimpleMeta;
import fr.doan.achilles.entity.metadata.WideMapMeta;

/**
 * PropertyMetaBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyMetaBuilder<K, V>
{
	private PropertyType type;
	private String propertyName;
	private Class<K> keyClass;
	private Class<V> valueClass;
	private Method[] accessors;
	private boolean lazy = false;
	private boolean singleKey = true;
	private boolean joinColumn = false;

	private List<Class<?>> componentClasses;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	public PropertyMetaBuilder(Class<K> keyClass, Class<V> valueClass) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
	}

	public static <K, V> PropertyMetaBuilder<K, V> builder(Class<K> keyClass, Class<V> valueClass)
	{
		return new PropertyMetaBuilder<K, V>(keyClass, valueClass);
	}

	public PropertyMetaBuilder<K, V> propertyName(String propertyName)
	{
		this.propertyName = propertyName;
		return this;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public PropertyMeta<K, V> build()
	{
		PropertyMeta<K, V> meta = null;
		switch (type)
		{
			case SIMPLE:
				meta = (PropertyMeta<K, V>) new SimpleMeta<V>();
				break;
			case LAZY_SIMPLE:
				meta = (PropertyMeta<K, V>) new SimpleLazyMeta<V>();
				break;
			case LIST:
				meta = (PropertyMeta<K, V>) new ListMeta<V>();
				break;
			case LAZY_LIST:
				meta = (PropertyMeta<K, V>) new ListLazyMeta<V>();
				break;
			case SET:
				meta = (PropertyMeta<K, V>) new SetMeta<V>();
				break;
			case LAZY_SET:
				meta = (PropertyMeta<K, V>) new SetLazyMeta<V>();
				break;
			case MAP:
				meta = new MapMeta<K, V>();
				break;
			case LAZY_MAP:
				meta = new MapLazyMeta<K, V>();
				break;
			case WIDE_MAP:
				if (singleKey)
				{
					meta = new WideMapMeta<K, V>();
				}
				else
				{
					meta = new MultiKeyWideMapMeta<K, V>();
				}
				break;
			case JOIN_WIDE_MAP:
				break;
			default:
				throw new IllegalStateException("The type '" + type
						+ "' is not supported for PropertyMeta builder");
		}

		meta.setPropertyName(propertyName);
		meta.setKeyClass(keyClass);
		if (keyClass != Void.class)
		{

			meta.setKeySerializer((Serializer) SerializerTypeInferer.getSerializer(keyClass));
		}
		meta.setValueClass(valueClass);
		meta.setValueSerializer((Serializer) SerializerTypeInferer.getSerializer(valueClass));
		meta.setGetter(accessors[0]);
		meta.setSetter(accessors[1]);
		meta.setLazy(lazy);
		meta.setSingleKey(singleKey);
		meta.setJoinColumn(joinColumn);
		meta.setComponentClasses(componentClasses);
		if (componentClasses != null && componentClasses.size() > 0)
		{
			List<Serializer<?>> componentSerializers = new ArrayList<Serializer<?>>();
			for (Class<?> componentClass : componentClasses)
			{
				componentSerializers.add((Serializer) SerializerTypeInferer
						.getSerializer(componentClass));
			}
			meta.setComponentSerializers(componentSerializers);
		}

		meta.setComponentGetters(componentGetters);
		meta.setComponentSetters(componentSetters);

		return meta;
	}

	public PropertyMetaBuilder<K, V> type(PropertyType type)
	{
		this.type = type;
		return this;
	}

	public PropertyMetaBuilder<K, V> accessors(Method[] accessors)
	{
		this.accessors = accessors;
		return this;
	}

	public PropertyMetaBuilder<K, V> lazy(boolean lazy)
	{
		this.lazy = lazy;
		return this;
	}

	public PropertyMetaBuilder<K, V> singleKey(boolean singleKey)
	{
		this.singleKey = singleKey;
		return this;
	}

	public PropertyMetaBuilder<K, V> joinColumn(boolean joinColumn)
	{
		this.joinColumn = joinColumn;
		return this;
	}

	public PropertyMetaBuilder<K, V> componentClasses(List<Class<?>> componentClasses)
	{
		this.componentClasses = componentClasses;
		return this;
	}

	public PropertyMetaBuilder<K, V> componentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
		return this;
	}

	public PropertyMetaBuilder<K, V> componentSetters(List<Method> componentSetters)
	{
		this.componentSetters = componentSetters;
		return this;
	}
}
