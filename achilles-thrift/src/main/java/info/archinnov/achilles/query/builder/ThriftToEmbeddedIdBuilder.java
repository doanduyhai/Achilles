package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * ThriftToEmbeddedIdBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftToEmbeddedIdBuilder<T> extends DefaultQueryBuilder<T>
{

    public ThriftToEmbeddedIdBuilder(ThriftQueryExecutor queryExecutor, Class<T> entityClass, EntityMeta meta,
            Object partitionKey, Object[] clusteringsTo)
    {
        super(queryExecutor, entityClass, meta);
        super.partitionKey(partitionKey);
        super.toClusteringsInternal(clusteringsTo);
    }

    /**
     * Set from embeddedId<br/>
     * <br/>
     * 
     * @param fromEmbeddedId
     *            From embeddedId
     * 
     * @return DefaultQueryBuilder<T>
     */
    public DefaultQueryBuilder<T> fromEmbeddedId(Object fromEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = mapper.fromCompoundToComponents(fromEmbeddedId,
                idMeta.getComponentGetters());
        List<Object> clusteringFrom = components.subList(1, components.size());

        super.fromClusteringsInternal(clusteringFrom.toArray(new Object[clusteringFrom.size()]));

        return this;
    }
}
