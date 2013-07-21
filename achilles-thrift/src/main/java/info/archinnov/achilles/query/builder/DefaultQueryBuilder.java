package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import java.util.Iterator;
import java.util.List;

/**
 * DefaultQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class DefaultQueryBuilder<T> extends AbstractQueryBuilder<T> {

    public DefaultQueryBuilder(ThriftQueryExecutor queryExecutor, Class<T> entityClass, EntityMeta meta) {
        super(queryExecutor, entityClass, meta);
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
    public DefaultQueryBuilder<T> ordering(OrderingMode ordering) {
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
    public DefaultQueryBuilder<T> bounding(BoundingMode boundingMode)
    {
        super.bounding(boundingMode);
        return this;
    }

    public DefaultQueryBuilder<T> consistencyLevel(ConsistencyLevel consistencyLevel)
    {
        super.consistencyLevel(consistencyLevel);
        return this;
    }

    public DefaultQueryBuilder<T> limit(int limit)
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
