package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;

/**
 * ThriftFromClusteringsBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftFromClusteringsBuilder<T> extends DefaultQueryBuilder<T> {

    public ThriftFromClusteringsBuilder(ThriftQueryExecutor queryExecutor,
            Class<T> entityClass,
            EntityMeta meta,
            Object partitionKey,
            Object... fromClusteringKeys) {
        super(queryExecutor, entityClass, meta);
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
