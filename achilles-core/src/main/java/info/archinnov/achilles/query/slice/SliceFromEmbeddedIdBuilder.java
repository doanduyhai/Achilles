package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.QueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * SliceFromEmbeddedIdBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceFromEmbeddedIdBuilder<T> extends DefaultQueryBuilder<T>
{
    public SliceFromEmbeddedIdBuilder(QueryExecutor queryExecutor,
            CompoundKeyValidator compoundKeyValidator,
            Class<T> entityClass, EntityMeta meta,
            Object partitionKey, Object[] clusteringsFrom)
    {
        super(queryExecutor, compoundKeyValidator, entityClass, meta);
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
    public DefaultQueryBuilder<T> toEmbeddedId(Object toEmbeddedId)
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
