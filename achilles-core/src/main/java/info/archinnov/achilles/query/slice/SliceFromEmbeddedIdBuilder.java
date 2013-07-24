package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * SliceFromEmbeddedIdBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceFromEmbeddedIdBuilder<CONTEXT extends PersistenceContext, T> extends
        DefaultQueryBuilder<CONTEXT, T>
{
    public SliceFromEmbeddedIdBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator,
            Class<T> entityClass, EntityMeta meta,
            Object partitionKey, Object[] clusteringsFrom)
    {
        super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
        super.partitionKey(partitionKey);
        super.fromClusteringsInternal(clusteringsFrom);
    }

    /**
     * Set to embeddedId<br/>
     * <br/>
     * 
     * @param toEmbeddedId
     *            To embeddedId
     * 
     * @return DefaultQueryBuilder<T>
     */
    public DefaultQueryBuilder<CONTEXT, T> toEmbeddedId(Object toEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
        List<Object> clusteringTo = components.subList(1, components.size());

        super.toClusteringsInternal(clusteringTo.toArray(new Object[clusteringTo.size()]));

        return this;
    }
}
