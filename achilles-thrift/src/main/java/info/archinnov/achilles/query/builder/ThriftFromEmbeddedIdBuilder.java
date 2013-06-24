package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * ThriftFromEmbeddedIdBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftFromEmbeddedIdBuilder<T> extends DefaultQueryBuilder<T>
{
    public ThriftFromEmbeddedIdBuilder(ThriftQueryExecutor queryExecutor, Class<T> entityClass, EntityMeta meta,
            Object partitionKey, Object[] clusteringsFrom)
    {
        super(queryExecutor, entityClass, meta);
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

        List<Object> components = mapper.fromCompoundToComponents(toEmbeddedId,
                idMeta.getComponentGetters());
        List<Object> clusteringTo = components.subList(1, components.size());

        super.toClusteringsInternal(clusteringTo.toArray(new Object[clusteringTo.size()]));

        return this;
    }
}
