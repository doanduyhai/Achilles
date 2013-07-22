package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.QueryExecutor;

/**
 * SliceFromClusteringsBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceFromClusteringsBuilder<T> extends DefaultQueryBuilder<T> {

    public SliceFromClusteringsBuilder(QueryExecutor queryExecutor,
            CompoundKeyValidator compoundKeyValidator,
            Class<T> entityClass,
            EntityMeta meta,
            Object partitionKey,
            Object... fromClusteringKeys) {
        super(queryExecutor, compoundKeyValidator, entityClass, meta);
        super.partitionKey(partitionKey);
        super.fromClusteringsInternal(fromClusteringKeys);
    }

    /**
     * Set to clustering components<br/>
     * <br/>
     * 
     * @param clusteringComponents
     *            To clustering components
     * 
     * @return DefaultQueryBuilder<T>
     */
    public DefaultQueryBuilder<T> toClusterings(Object... clusteringComponents)
    {
        super.toClusteringsInternal(clusteringComponents);
        return this;
    }
}
