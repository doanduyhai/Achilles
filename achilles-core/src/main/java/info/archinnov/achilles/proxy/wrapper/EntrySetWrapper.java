package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.proxy.wrapper.builder.EntryIteratorWrapperBuilder;
import info.archinnov.achilles.proxy.wrapper.builder.MapEntryWrapperBuilder;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntrySetWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntrySetWrapper extends AbstractWrapper implements Set<Entry<Object, Object>>
{
    private static final Logger log = LoggerFactory.getLogger(EntrySetWrapper.class);

    private Set<Entry<Object, Object>> target;

    public EntrySetWrapper(Set<Entry<Object, Object>> target) {
        this.target = target;
    }

    @Override
    public boolean add(Entry<Object, Object> arg0)
    {
        throw new UnsupportedOperationException("This method is not supported for an Entry set");
    }

    @Override
    public boolean addAll(Collection<? extends Entry<Object, Object>> arg0)
    {
        throw new UnsupportedOperationException("This method is not supported for an Entry set");
    }

    @Override
    public void clear()
    {
        log.trace("Mark dirty for property {} of entity class {} upon entry set clearance",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        this.target.clear();
        this.markDirty();
    }

    @Override
    public boolean contains(Object arg0)
    {
        return this.target.contains(proxifier.unwrap(arg0));
    }

    @Override
    public boolean containsAll(Collection<?> arg0)
    {
        return this.target.containsAll(proxifier.unwrap(arg0));
    }

    @Override
    public boolean isEmpty()
    {
        return this.target.isEmpty();
    }

    @Override
    public Iterator<Entry<Object, Object>> iterator()
    {
        log.trace("Build iterator wrapper for entry set of property {} of entity class {}",
                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
        return EntryIteratorWrapperBuilder //
                .builder(context, this.target.iterator())
                .dirtyMap(dirtyMap)
                .setter(setter)
                .propertyMeta(propertyMeta)
                .proxifier(proxifier)
                .build();
    }

    @Override
    public boolean remove(Object arg0)
    {
        boolean result = false;
        result = this.target.remove(proxifier.unwrap(arg0));
        if (result)
        {
            log.trace("Mark dirty for property {} of entity class {} upon entry removal",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.markDirty();
        }
        return result;
    }

    @Override
    public boolean removeAll(Collection<?> arg0)
    {
        boolean result = false;
        result = this.target.removeAll(proxifier.unwrap(arg0));
        if (result)
        {
            log.trace("Mark dirty for property {} of entity class {} upon all entries removal",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.markDirty();
        }
        return result;
    }

    @Override
    public boolean retainAll(Collection<?> arg0)
    {
        boolean result = false;
        result = this.target.retainAll(proxifier.unwrap(arg0));
        if (result)
        {
            log.trace("Mark dirty for property {} of entity class {} upon entries retaining",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            this.markDirty();
        }
        return result;
    }

    @Override
    public int size()
    {
        return this.target.size();
    }

    @Override
    public Object[] toArray()
    {
        Object[] result = null;
        if (isJoin())
        {
            log
                    .trace("Build proxies for join entities of entrey set property {} of entity class {} upon toArray() call",
                            propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            Object[] array = new MapEntryWrapper[this.target.size()];
            int i = 0;
            for (Map.Entry<Object, Object> entry : this.target)
            {
                array[i] = MapEntryWrapperBuilder //
                        .builder(context, entry)
                        .dirtyMap(dirtyMap)
                        .setter(setter)
                        .propertyMeta(propertyMeta)
                        .proxifier(proxifier)
                        .build();
                i++;
            }
            result = array;
        }
        else
        {
            result = this.target.toArray();
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] arg0)
    {
        T[] result = null;
        if (isJoin())
        {
            T[] array = this.target.toArray(arg0);

            for (int i = 0; i < array.length; i++)
            {
                log
                        .trace("Build proxies for join entities of entrey set property {} of entity class {} upon toArray(T[] arg) call",
                                propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

                array[i] = (T) MapEntryWrapperBuilder
                        .builder(context, (Entry<Object, Object>) array[i])
                        .dirtyMap(dirtyMap)
                        .setter(setter)
                        .propertyMeta(propertyMeta)
                        .proxifier(proxifier)
                        .build();
            }
            result = array;
        }
        else
        {
            result = this.target.toArray(arg0);
        }
        return result;
    }

}
