package info.archinnov.achilles.query;

import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.Arrays;
import java.util.List;

/**
 * SliceQuery
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQuery<T> {
    private SliceQueryBuilder queryBuilder = new SliceQueryBuilder();

    private ConfigurationContext configContext;
    private CQLDaoContext daoContext;

    private EntityMeta entityMeta;
    private PropertyMeta<?, ?> idMeta;

    private Class<T> entityClass = null;
    private List<Object> partitionKeys;
    private Object from = null;
    private Object to = null;
    private BoundingMode boundingMode = null;
    private OrderingMode orderingMode = null;
    private int limit = 100;
    private ConsistencyLevel readLevel = null;

    public SliceQuery(EntityMeta entityMeta, ConfigurationContext configContext, CQLDaoContext daoContext,
            Class<T> entityClass) {
        this.entityMeta = entityMeta;
        this.configContext = configContext;
        this.daoContext = daoContext;
        this.entityClass = entityClass;
        idMeta = entityMeta.getIdMeta();
    }

    public SliceQuery<T> partitionKeys(Object... partitionKeys) {
        Validator.validateNotNull(partitionKeys, "Partition keys should not be null for slice query");
        Class<?> partitionType;
        if (idMeta.isCompound()) {
            partitionType = idMeta.getComponentClasses().get(0);
        } else {
            partitionType = idMeta.getValueClass();
        }

        for (Object partitionKey : partitionKeys) {
            if (partitionKey == null) {
                throw new IllegalArgumentException("Partition keys should not be null");
            }
            Validator.validateTrue(partitionKey == partitionType, "Partition keys should be of type '"
                    + partitionType.getCanonicalName() + "'");

        }
        this.partitionKeys = Arrays.<Object> asList(partitionKeys);
        return this;
    }

    public SliceQuery<T> from(Object from) {
        Validator.validateNotNull(from, "From compound key should not be null for slice query");
        Class<?> compoundType = idMeta.getValueClass();
        Validator.validateTrue(from.getClass() == compoundType, "Start compound key '" + from
                + "' should be of type '" + compoundType.getCanonicalName() + "'");
        this.from = from;
        return this;
    }

    public SliceQuery<T> to(Object to) {
        Validator.validateNotNull(to, "From compound key should not be null");
        Class<?> compoundType = idMeta.getValueClass();
        Validator.validateTrue(to.getClass() == compoundType, "End compound key '" + to + "' should be of type '"
                + compoundType.getCanonicalName() + "'");
        this.to = to;
        return this;
    }

    public SliceQuery<T> bounding(BoundingMode boundingMode) {
        Validator.validateNotNull(boundingMode, "Bounding mode provided for Slice Query should not be null");
        this.boundingMode = boundingMode;
        return this;
    }

    public SliceQuery<T> ordering(OrderingMode orderingMode) {
        Validator.validateNotNull(orderingMode, "Ordering mode provided for Slice Query should not be null");
        this.orderingMode = orderingMode;
        return this;
    }

    public SliceQuery<T> limit(int limit) {
        Validator.validateTrue(limit > 0, "Limit provided for Slice Query should be strictly positive");
        this.limit = limit;
        return this;
    }

    public SliceQuery<T> consistency(ConsistencyLevel readLevel) {
        Validator.validateNotNull(readLevel, "Consistency level provided for Slice Query should not be null");
        this.readLevel = readLevel;
        return this;
    }

    public List<T> findAll() {
        return null;
    }

    public List<T> findFirst(int n) {
        return null;
    }

}
