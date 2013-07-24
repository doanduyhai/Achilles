package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;
import java.util.Iterator;
import java.util.List;

/**
 * DefaultQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class DefaultQueryBuilder<CONTEXT extends PersistenceContext, T> extends RootSliceQueryBuilder<CONTEXT, T> {

    public DefaultQueryBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator,
            Class<T> entityClass, EntityMeta meta) {
        super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
    }

    /**
     * Set ordering<br/>
     * <br/>
     * 
     * @param ordering
     *            ordering mode: ASCENDING or DESCENDING
     * 
     * @return DefaultQueryBuilder<T>
     */
    public DefaultQueryBuilder<CONTEXT, T> ordering(OrderingMode ordering) {
        super.ordering(ordering);
        return this;
    }

    /**
     * Set bounding mode<br/>
     * <br/>
     * 
     * @param boundingMode
     *            bounding mode: ASCENDING or DESCENDING
     * 
     * @return DefaultQueryBuilder<T>
     */
    public DefaultQueryBuilder<CONTEXT, T> bounding(BoundingMode boundingMode)
    {
        super.bounding(boundingMode);
        return this;
    }

    public DefaultQueryBuilder<CONTEXT, T> consistencyLevel(ConsistencyLevel consistencyLevel)
    {
        super.consistencyLevel(consistencyLevel);
        return this;
    }

    public DefaultQueryBuilder<CONTEXT, T> limit(int limit)
    {
        super.limit(limit);
        return this;
    }

    public List<T> get()
    {
        return super.get();
    }

    public List<T> get(int n)
    {
        return super.get(n);
    }

    public Iterator<T> iterator()
    {
        return super.iterator();
    }

    public Iterator<T> iterator(int batchSize)
    {
        return super.iterator(batchSize);
    }

    public void remove()
    {
        super.remove();
    }

    public void remove(int n)
    {
        super.remove(n);
    }
}
