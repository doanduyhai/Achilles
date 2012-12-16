package fr.doan.achilles.entity.metadata.builder;

import java.lang.reflect.Method;
import java.util.List;

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
	private Serializer<K> keySerializer;
	private Class<V> valueClass;
	private Serializer<?> valueSerializer;
	private Method getter;
	private Method setter;
	private boolean lazy;
	private boolean singleKey;
	private boolean joinColumn;

	private List<Serializer<?>> componentSerializers;
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

	@SuppressWarnings("unchecked")
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
		meta.setKeySerializer(keySerializer);
		meta.setValueClass(valueClass);
		meta.setValueSerializer(valueSerializer);
		meta.setGetter(getter);
		meta.setSetter(setter);
		meta.setLazy(lazy);
		meta.setSingleKey(singleKey);
		meta.setJoinColumn(joinColumn);
		meta.setComponentSerializers(componentSerializers);
		meta.setComponentGetters(componentGetters);
		meta.setComponentSetters(componentSetters);

		return meta;
	}

	public PropertyMetaBuilder<K, V> type(PropertyType type)
	{
		this.type = type;
		return this;
	}

	public PropertyMetaBuilder<K, V> keySerializer(Serializer<K> keySerializer)
	{
		this.keySerializer = keySerializer;
		return this;
	}

	public PropertyMetaBuilder<K, V> valueSerializer(Serializer<V> valueSerializer)
	{
		this.valueSerializer = valueSerializer;
		return this;
	}

	public PropertyMetaBuilder<K, V> getter(Method getter)
	{
		this.getter = getter;
		return this;
	}

	public PropertyMetaBuilder<K, V> setter(Method setter)
	{
		this.setter = setter;
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

	public PropertyMetaBuilder<K, V> componentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
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
