package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.query.SliceQuery.*;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.Iterator;
import java.util.List;

public abstract class RootSliceQueryBuilder<CONTEXT extends PersistenceContext, T>
{

    protected SliceQueryExecutor<CONTEXT> sliceQueryExecutor;
    protected CompoundKeyValidator compoundKeyValidator;
    protected Class<T> entityClass;
    protected EntityMeta meta;

    protected Object partitionKey = null;
    private PropertyMeta<?, ?> idMeta;
    private Object[] fromClusterings = null;
    private Object[] toClusterings = null;
    private OrderingMode ordering = OrderingMode.ASCENDING;
    private BoundingMode bounding = BoundingMode.INCLUSIVE_BOUNDS;
    private ConsistencyLevel consistencyLevel;
    private int limit = DEFAULT_LIMIT;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private boolean limitHasBeenSet = false;
    private boolean orderingHasBeenSet = false;

    RootSliceQueryBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator,
            Class<T> entityClass, EntityMeta meta)
    {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.compoundKeyValidator = compoundKeyValidator;
        this.entityClass = entityClass;
        this.meta = meta;
        this.idMeta = meta.getIdMeta();
    }

    protected RootSliceQueryBuilder<CONTEXT, T> partitionKeyInternal(Object partitionKey)
    {
        compoundKeyValidator.validatePartitionKey(idMeta, partitionKey);
        this.partitionKey = partitionKey;
        return this;
    }

    protected RootSliceQueryBuilder<CONTEXT, T> fromClusteringsInternal(Object... clusteringComponents)
    {
        compoundKeyValidator.validateClusteringKeys(idMeta, clusteringComponents);
        fromClusterings = clusteringComponents;
        return this;
    }

    protected RootSliceQueryBuilder<CONTEXT, T> toClusteringsInternal(Object... clusteringComponents)
    {
        compoundKeyValidator.validateClusteringKeys(idMeta, clusteringComponents);
        toClusterings = clusteringComponents;
        return this;
    }

    protected RootSliceQueryBuilder<CONTEXT, T> ordering(OrderingMode ordering)
    {
        Validator.validateNotNull(ordering,
                "Ordering mode for slice query for entity '" + meta.getClassName()
                        + "' should not be null");
        this.ordering = ordering;
        orderingHasBeenSet = true;
        return this;
    }

    protected RootSliceQueryBuilder<CONTEXT, T> bounding(BoundingMode boundingMode)
    {
        Validator.validateNotNull(boundingMode,
                "Bounding mode for slice query for entity '" + meta.getClassName()
                        + "' should not be null");
        bounding = boundingMode;

        return this;
    }

    protected RootSliceQueryBuilder<CONTEXT, T> consistencyLevel(ConsistencyLevel consistencyLevel)
    {
        Validator.validateNotNull(consistencyLevel,
                "ConsistencyLevel for slice query for entity '" + meta.getClassName()
                        + "' should not be null");
        this.consistencyLevel = consistencyLevel;

        return this;
    }

    protected RootSliceQueryBuilder<CONTEXT, T> limit(int limit)
    {
        this.limit = limit;
        limitHasBeenSet = true;
        return this;
    }

    protected List<T> get()
    {
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.get(clusteredQuery);
    }

    protected List<T> get(int n)
    {
        limit = n;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.get(clusteredQuery);
    }

    protected T getFirstOccurence(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling getFirst()");
        limit = 1;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        List<T> result = sliceQueryExecutor.get(clusteredQuery);
        if (result.isEmpty())
            return null;
        else
            return result.get(0);
    }

    protected List<T> getFirst(int n, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling getFirst(int n)");
        limit = n;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.get(clusteredQuery);
    }

    protected T getLastOccurence(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(orderingHasBeenSet,
                "You should not set 'ordering' parameter when calling getLast()");
        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling getLast()");
        limit = 1;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        List<T> result = sliceQueryExecutor.get(clusteredQuery);
        if (result.isEmpty())
            return null;
        else
            return result.get(0);
    }

    protected List<T> getLast(int n, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(orderingHasBeenSet,
                "You should not set 'ordering' parameter when calling getLast(int n)");
        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling getLast(int n)");
        limit = n;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.get(clusteredQuery);
    }

    protected Iterator<T> iterator()
    {
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.iterator(clusteredQuery);
    }

    protected Iterator<T> iteratorWithComponents(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.iterator(clusteredQuery);
    }

    protected Iterator<T> iterator(int batchSize)
    {
        this.batchSize = batchSize;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.iterator(clusteredQuery);
    }

    protected Iterator<T> iteratorWithComponents(int batchSize, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);
        this.batchSize = batchSize;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        return sliceQueryExecutor.iterator(clusteredQuery);
    }

    protected void remove()
    {
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        sliceQueryExecutor.remove(clusteredQuery);
    }

    protected void remove(int n)
    {
        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling remove(int n)");
        limit = n;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        sliceQueryExecutor.remove(clusteredQuery);
    }

    protected void removeFirstOccurence(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling removeFirst()");
        limit = 1;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        sliceQueryExecutor.remove(clusteredQuery);
    }

    protected void removeFirst(int n, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling removeFirst(int n)");
        limit = n;
        limitHasBeenSet = true;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        sliceQueryExecutor.remove(clusteredQuery);
    }

    protected void removeLastOccurence(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(orderingHasBeenSet,
                "You should not set 'ordering' parameter when calling removeLast()");
        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling removeLast()");
        limit = 1;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        sliceQueryExecutor.remove(clusteredQuery);
    }

    protected void removeLast(int n, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(orderingHasBeenSet,
                "You should not set 'ordering' parameter when calling removeLast(int n)");
        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling removeLast(int n)");
        limit = n;
        limitHasBeenSet = true;
        ordering = OrderingMode.DESCENDING;
        SliceQuery<T> clusteredQuery = buildClusterQuery();
        sliceQueryExecutor.remove(clusteredQuery);
    }

    protected SliceQuery<T> buildClusterQuery()
    {
        return new SliceQuery<T>(entityClass, meta, partitionKey, fromClusterings,
                toClusterings, ordering, bounding, consistencyLevel, limit, batchSize, limitHasBeenSet);
    }
}
