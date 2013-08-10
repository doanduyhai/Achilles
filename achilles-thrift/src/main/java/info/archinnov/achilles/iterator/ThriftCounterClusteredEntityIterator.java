package info.archinnov.achilles.iterator;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCounterClusteredEntityIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftCounterClusteredEntityIterator<T> extends ThriftAbstractClusteredEntityIterator<T>
{
    private static final Logger log = LoggerFactory.getLogger(ThriftCounterClusteredEntityIterator.class);

    private ThriftCounterSliceIterator<Object> sliceIterator;

    public ThriftCounterClusteredEntityIterator(Class<T> entityClass,
            ThriftCounterSliceIterator<Object> sliceIterator,
            ThriftPersistenceContext context)
    {
        super(entityClass, sliceIterator, context);
        this.sliceIterator = sliceIterator;
    }

    @Override
    public T next()
    {
        log.trace("Get next clustered entity of type {} ", entityClass.getCanonicalName());
        HCounterColumn<Composite> counterColumn = this.sliceIterator.next();
        T target = transformer.buildClusteredEntityWithIdOnly(entityClass, context, counterColumn.getName()
                .getComponents());
        return proxifyClusteredEntity(target);
    }

}
