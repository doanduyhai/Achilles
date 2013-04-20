package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.builder.EntrySetWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.KeySetWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.ValueCollectionWrapperBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class MapWrapper<ID, K, V> extends AbstractWrapper<ID, K, V> implements Map<K, V>
{
	private static final Logger log = LoggerFactory.getLogger(MapWrapper.class);

	private final Map<K, V> target;

	public MapWrapper(Map<K, V> target) {
		this.target = target;
	}

	@Override
	public void clear()
	{
		if (this.target.size() > 0)
		{
			log.trace("Mark map property {} of entity class {} dirty upon all elements clearance",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
			this.markDirty();
		}
		this.target.clear();

	}

	@Override
	public boolean containsKey(Object key)
	{
		return this.target.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value)
	{
		return this.target.containsValue(proxifier.unproxy(value));
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet()
	{
		Set<Entry<K, V>> targetEntrySet = this.target.entrySet();
		if (targetEntrySet.size() > 0)
		{
			log.trace("Build map entry wrapper for map property {} of entity class {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

			EntrySetWrapper<ID, K, V> wrapperSet = EntrySetWrapperBuilder //
					.builder(context, targetEntrySet) //
					.dirtyMap(dirtyMap) //
					.setter(setter) //
					.propertyMeta(propertyMeta) //
					.proxifier(proxifier) //
					.build();
			targetEntrySet = wrapperSet;
		}
		return targetEntrySet;
	}

	@Override
	public V get(Object key)
	{
		log.trace("Return value having key{} for map property {} of entity class {}", key,
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
		if (isJoin())
		{
			V joinEntity = this.target.get(key);
			return proxifier.buildProxy(joinEntity, joinContext(joinEntity));
		}
		else
		{
			return this.target.get(key);
		}
	}

	@Override
	public boolean isEmpty()
	{
		return this.target.isEmpty();
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public Set<K> keySet()
	{
		Set<K> keySet = this.target.keySet();
		if (keySet.size() > 0)
		{
			log.trace("Build key set wrapper for map property {} of entity class {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

			KeySetWrapper<ID, K> keySetWrapper = KeySetWrapperBuilder //
					.builder(context, keySet) //
					.dirtyMap(dirtyMap) //
					.setter(setter) //
					.propertyMeta((PropertyMeta) propertyMeta) //
					.proxifier(proxifier) //
					.build();
			keySet = keySetWrapper;
		}
		return keySet;
	}

	@Override
	public V put(K key, V value)
	{
		log.trace(
				"Mark map property {} of entity class {} dirty upon new value {} addition for key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), value, key);

		V result = this.target.put(key, proxifier.unproxy(value));
		this.markDirty();
		return result;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m)
	{
		Map<K, V> map = new HashMap<K, V>();
		for (Entry<? extends K, ? extends V> entry : m.entrySet())
		{
			map.put(entry.getKey(), proxifier.unproxy(entry.getValue()));
		}

		log.trace(
				"Mark map property {} of entity class {} dirty upon new key/value pairs addition",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

		this.target.putAll(map);
		this.markDirty();
	}

	@Override
	public V remove(Object key)
	{
		Object unproxy = proxifier.unproxy(key);
		if (this.target.containsKey(unproxy))
		{
			log.trace(
					"Mark map property {} of entity class {} dirty upon removal of value havo,g key {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), key);
			this.markDirty();
		}
		return this.target.remove(unproxy);
	}

	@Override
	public int size()
	{
		return this.target.size();
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	@Override
	public Collection<V> values()
	{
		Collection<V> values = this.target.values();

		if (values.size() > 0)
		{
			log.trace("Build values collection wrapper for map property {} of entity class {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

			ValueCollectionWrapper<ID, V> collectionWrapper = ValueCollectionWrapperBuilder //
					.builder(context, values) //
					.dirtyMap(dirtyMap) //
					.setter(setter) //
					.propertyMeta((PropertyMeta) propertyMeta) //
					.proxifier(proxifier) //
					.build();
			values = collectionWrapper;
		}
		return values;
	}

	public Map<K, V> getTarget()
	{
		return target;
	}
}
