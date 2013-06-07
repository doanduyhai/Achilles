package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.proxy.wrapper.builder.MapEntryWrapperBuilder;
import java.util.Iterator;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntryIteratorWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class EntryIteratorWrapper<K, V> extends AbstractWrapper<K, V> implements Iterator<Entry<K, V>> {
    private static final Logger log = LoggerFactory.getLogger(EntryIteratorWrapper.class);

    private Iterator<Entry<K, V>> target;

    public EntryIteratorWrapper(Iterator<Entry<K, V>> target) {
        this.target = target;
    }

    @Override
    public boolean hasNext() {
        return this.target.hasNext();
    }

    @Override
    public Entry<K, V> next() {
        Entry<K, V> result = null;
        Entry<K, V> entry = this.target.next();
        if (entry != null) {
            log.trace("Build wrapper for next entry of property {} of entity class {}",
                    propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
            result = MapEntryWrapperBuilder.builder(context, entry) //
                    .dirtyMap(dirtyMap)
                    //
                    .setter(setter)
                    //
                    .propertyMeta(propertyMeta)
                    //
                    .proxifier(proxifier)
                    //
                    .build();
        }
        return result;
    }

    @Override
    public void remove() {
        log.trace("Mark dirty for property {} of entity class {} upon entry removal", propertyMeta.getPropertyName(),
                propertyMeta.getEntityClassName());
        this.target.remove();
        this.markDirty();
    }

}
