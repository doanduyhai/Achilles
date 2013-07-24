package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;

/**
 * SliceToClusteringsBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceToClusteringsBuilder<CONTEXT extends PersistenceContext, T> extends DefaultQueryBuilder<CONTEXT, T> {

    public SliceToClusteringsBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator, Class<T> entityClass,
            EntityMeta meta,
            Object partitionKey,
            Object... toClusteringKeys) {
        super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
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
    public DefaultQueryBuilder<CONTEXT, T> fromClusterings(Object... clusteringComponents)
    {
        super.fromClusteringsInternal(clusteringComponents);
        return this;
    }
}
