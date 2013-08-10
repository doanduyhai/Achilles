package info.archinnov.achilles.iterator;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftClusteredEntityIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftClusteredEntityIterator<T> extends ThriftAbstractClusteredEntityIterator<T>
{
    private static final Logger log = LoggerFactory.getLogger(ThriftClusteredEntityIterator.class);

    private ThriftAbstractSliceIterator<HColumn<Composite, Object>> sliceIterator;

    public ThriftClusteredEntityIterator(Class<T> entityClass,
            ThriftAbstractSliceIterator<HColumn<Composite, Object>> sliceIterator,
            ThriftPersistenceContext context)
    {
        super(entityClass, sliceIterator, context);
        this.sliceIterator = sliceIterator;
    }

    @Override
    public T next()
    {
        log.trace("Get next clustered entity of type {} ", entityClass.getCanonicalName());
        HColumn<Composite, Object> hColumn = this.sliceIterator.next();
        T target;
        if (context.isValueless())
        {
            target = transformer.buildClusteredEntityWithIdOnly(entityClass, context, hColumn.getName()
                    .getComponents());
        }
        else
        {
            target = transformer.buildClusteredEntity(entityClass, context, hColumn);
        }
        return proxifyClusteredEntity(target);

    }

}
