package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;

/**
 * ThriftToClusteringsBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftToClusteringsBuilder<T> extends DefaultQueryBuilder<T> {

    public ThriftToClusteringsBuilder(ThriftQueryExecutor queryExecutor, Class<T> entityClass,
            EntityMeta meta,
            Object partitionKey,
            Object... toClusteringKeys) {
        super(queryExecutor, entityClass, meta);
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
