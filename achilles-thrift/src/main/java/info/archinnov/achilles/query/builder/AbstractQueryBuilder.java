package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;
import info.archinnov.achilles.query.ClusteredQuery;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractQueryBuilder<T>
{

    protected ThriftQueryExecutor queryExecutor;
    protected Class<T> entityClass;
    protected EntityMeta meta;

    protected Object partitionKey = null;
    private PropertyMeta<?, ?> idMeta;
    private Object[] fromClusterings = null;
    private Object[] toClusterings = null;
    private OrderingMode ordering = OrderingMode.ASCENDING;
    private BoundingMode bounding = BoundingMode.INCLUSIVE_BOUNDS;
    private ConsistencyLevel consistencyLevel;
    private int limit = ThriftAbstractDao.DEFAULT_LENGTH;
    private int batchSize = ThriftAbstractDao.DEFAULT_LENGTH;
    private boolean limitHasBeenSet = false;
    private boolean orderingHasBeenSet = false;

    private ThriftCompoundKeyValidator compoundKeyValidator = new ThriftCompoundKeyValidator();
    protected ThriftCompoundKeyMapper mapper = new ThriftCompoundKeyMapper();

    AbstractQueryBuilder(ThriftQueryExecutor queryExecutor, Class<T> entityClass,
            EntityMeta meta)
    {
        this.queryExecutor = queryExecutor;
        this.entityClass = entityClass;
        this.meta = meta;
        this.idMeta = meta.getIdMeta();
    }

    protected AbstractQueryBuilder<T> partitionKey(Object partitionKey)
    {
        compoundKeyValidator.validatePartitionKey(idMeta, partitionKey);
        this.partitionKey = partitionKey;
        return this;
    }

    protected AbstractQueryBuilder<T> fromClusteringsInternal(Object... clusteringComponents)
    {
        compoundKeyValidator.validateClusteringKeys(idMeta, clusteringComponents);
        fromClusterings = clusteringComponents;
        return this;
    }

    protected AbstractQueryBuilder<T> toClusteringsInternal(Object... clusteringComponents)
    {
        compoundKeyValidator.validateClusteringKeys(idMeta, clusteringComponents);
        toClusterings = clusteringComponents;
        return this;
    }

    protected AbstractQueryBuilder<T> ordering(OrderingMode ordering)
    {
        Validator.validateNotNull(ordering,
                "Ordering mode for slice query for entity '" + meta.getClassName()
                        + "' should not be null");
        this.ordering = ordering;
        orderingHasBeenSet = true;
        return this;
    }

    protected AbstractQueryBuilder<T> bounding(BoundingMode boundingMode)
    {
        Validator.validateNotNull(boundingMode,
                "Bounding mode for slice query for entity '" + meta.getClassName()
                        + "' should not be null");
        bounding = boundingMode;

        return this;
    }

    protected AbstractQueryBuilder<T> consistencyLevel(ConsistencyLevel consistencyLevel)
    {
        Validator.validateNotNull(consistencyLevel,
                "ConsistencyLevel for slice query for entity '" + meta.getClassName()
                        + "' should not be null");
        this.consistencyLevel = consistencyLevel;

        return this;
    }

    protected AbstractQueryBuilder<T> limit(int limit)
    {
        this.limit = limit;
        limitHasBeenSet = true;
        return this;
    }

    protected List<T> get()
    {
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.get(clusteredQuery);
    }

    protected List<T> get(int n)
    {
        limit = n;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.get(clusteredQuery);
    }

    protected T getFirstOccurence(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling getFirst()");
        limit = 1;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        List<T> result = queryExecutor.get(clusteredQuery);
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
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.get(clusteredQuery);
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
        ordering = OrderingMode.DESCENDING;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        List<T> result = queryExecutor.get(clusteredQuery);
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
        ordering = OrderingMode.DESCENDING;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.get(clusteredQuery);
    }

    protected Iterator<T> iterator()
    {
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.iterator(clusteredQuery);
    }

    protected Iterator<T> iteratorWithComponents(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.iterator(clusteredQuery);
    }

    protected Iterator<T> iterator(int batchSize)
    {
        this.batchSize = batchSize;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.iterator(clusteredQuery);
    }

    protected Iterator<T> iteratorWithComponents(int batchSize, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);
        this.batchSize = batchSize;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        return queryExecutor.iterator(clusteredQuery);
    }

    protected void remove()
    {
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        queryExecutor.remove(clusteredQuery);
    }

    protected void remove(int n)
    {
        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling remove(int n)");
        limit = n;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        queryExecutor.remove(clusteredQuery);
    }

    protected void removeFirstOccurence(Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling removeFirst()");
        limit = 1;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        queryExecutor.remove(clusteredQuery);
    }

    protected void removeFirst(int n, Object... clusteringComponents)
    {
        fromClusteringsInternal(clusteringComponents);
        toClusteringsInternal(clusteringComponents);

        Validator.validateFalse(limitHasBeenSet,
                "You should not set 'limit' parameter when calling removeFirst(int n)");
        limit = n;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        queryExecutor.remove(clusteredQuery);
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
        ordering = OrderingMode.DESCENDING;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        queryExecutor.remove(clusteredQuery);
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
        ordering = OrderingMode.DESCENDING;
        ClusteredQuery<T> clusteredQuery = buildClusterQuery();
        queryExecutor.remove(clusteredQuery);
    }

    protected ClusteredQuery<T> buildClusterQuery()
    {
        return new ClusteredQuery<T>(entityClass, meta, partitionKey, fromClusterings,
                toClusterings, ordering,
                bounding, consistencyLevel, limit, batchSize);
    }
}
