package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.wrapper.builder.EntrySetWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.KeySetWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.ValueCollectionWrapperBuilder;
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
public class MapWrapper extends AbstractWrapper implements Map<Object, Object>
{
    private static final Logger log = LoggerFactory.getLogger(MapWrapper.class);

    private final Map<Object, Object> target;

    public MapWrapper(Map<Object, Object> target) {
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
        return this.target.containsValue(proxifier.unwrap(value));
    }

    @Override
    public Set<java.util.Map.Entry<Object, Object>> entrySet()
    {
        Set<Entry<Object, Object>> targetEntrySet = this.target.entrySet();
        if (targetEntrySet.size() > 0)
        {
            log.trace("Build map entry wrapper for map property {} of entity class {}",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

            EntrySetWrapper wrapperSet = EntrySetWrapperBuilder //
                    .builder(context, targetEntrySet)
                    .dirtyMap(dirtyMap)
                    .setter(setter)
                    .propertyMeta(propertyMeta)
                    .proxifier(proxifier)
                    .build();
            targetEntrySet = wrapperSet;
        }
        return targetEntrySet;
    }

    @Override
    public Object get(Object key)
    {
        log.trace("Return value having key{} for map property {} of entity class {}", key,
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        if (isJoin())
        {
            Object joinEntity = this.target.get(key);
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

    @Override
    public Set<Object> keySet()
    {
        Set<Object> keySet = this.target.keySet();
        if (keySet.size() > 0)
        {
            log.trace("Build key set wrapper for map property {} of entity class {}",
                    propertyMeta.getPropertyName(),
                    propertyMeta.getEntityClassName());

            @SuppressWarnings(
            {
                    "rawtypes",
                    "unchecked"
            })
            KeySetWrapper keySetWrapper = KeySetWrapperBuilder
                    .builder(context, keySet)
                    .dirtyMap(dirtyMap)
                    .setter(setter)
                    .propertyMeta((PropertyMeta) propertyMeta)
                    .proxifier(proxifier)
                    .build();
            keySet = keySetWrapper;
        }
        return keySet;
    }

    @Override
    public Object put(Object key, Object value)
    {
        log
                .trace("Mark map property {} of entity class {} dirty upon new value {} addition for key {}",
                        propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), value,
                        key);

        Object result = this.target.put(key, proxifier.unwrap(value));
        this.markDirty();
        return result;
    }

    @Override
    public void putAll(Map<?, ?> m)
    {
        Map<Object, Object> map = new HashMap<Object, Object>();
        for (Entry<?, ?> entry : m.entrySet())
        {
            map.put(entry.getKey(), proxifier.unwrap(entry.getValue()));
        }

        log.trace(
                "Mark map property {} of entity class {} dirty upon new key/value pairs addition",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

        this.target.putAll(map);
        this.markDirty();
    }

    @Override
    public Object remove(Object key)
    {
        Object unproxy = proxifier.unwrap(key);
        if (this.target.containsKey(unproxy))
        {
            log
                    .trace("Mark map property {} of entity class {} dirty upon removal of value havo,g key {}",
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

    @Override
    public Collection<Object> values()
    {
        Collection<Object> values = this.target.values();

        if (values.size() > 0)
        {
            log.trace("Build values collection wrapper for map property {} of entity class {}",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

            @SuppressWarnings(
            {
                    "rawtypes",
                    "unchecked"
            })
            ValueCollectionWrapper collectionWrapper = ValueCollectionWrapperBuilder //
                    .builder(context, values)
                    .dirtyMap(dirtyMap)
                    .setter(setter)
                    .propertyMeta((PropertyMeta) propertyMeta)
                    .proxifier(proxifier)
                    .build();
            values = collectionWrapper;
        }
        return values;
    }

    public Map<Object, Object> getTarget()
    {
        return target;
    }
}
