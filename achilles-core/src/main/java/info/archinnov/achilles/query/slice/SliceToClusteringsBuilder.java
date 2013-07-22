package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.QueryExecutor;

/**
 * SliceToClusteringsBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceToClusteringsBuilder<T> extends DefaultQueryBuilder<T> {

    public SliceToClusteringsBuilder(QueryExecutor queryExecutor,
            CompoundKeyValidator compoundKeyValidator, Class<T> entityClass,
            EntityMeta meta,
            Object partitionKey,
            Object... toClusteringKeys) {
        super(queryExecutor, compoundKeyValidator, entityClass, meta);
        super.partitionKey(partitionKey);
        super.toClusteringsInternal(toClusteringKeys);
    }

    /**
     * Set from clustering components<br/>
     * <br/>
     * 
     * @param clusteringComponents
     *            From clustering components
     * 
     * @return DefaultQueryBuilder<T>
     */
    public DefaultQueryBuilder<T> fromClusterings(Object... clusteringComponents)
    {
        super.fromClusteringsInternal(clusteringComponents);
        return this;
    }
}
