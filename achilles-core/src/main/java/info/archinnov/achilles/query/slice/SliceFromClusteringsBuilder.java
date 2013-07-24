package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;

/**
 * SliceFromClusteringsBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceFromClusteringsBuilder<CONTEXT extends PersistenceContext, T> extends
        DefaultQueryBuilder<CONTEXT, T> {

    public SliceFromClusteringsBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator,
            Class<T> entityClass,
            EntityMeta meta,
            Object partitionKey,
            Object... fromClusteringKeys) {
        super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
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
    public DefaultQueryBuilder<CONTEXT, T> toClusterings(Object... clusteringComponents)
    {
        super.toClusteringsInternal(clusteringComponents);
        return this;
    }
}
