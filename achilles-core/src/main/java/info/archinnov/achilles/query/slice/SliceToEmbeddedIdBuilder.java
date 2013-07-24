package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * SliceToEmbeddedIdBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceToEmbeddedIdBuilder<CONTEXT extends PersistenceContext, T> extends DefaultQueryBuilder<CONTEXT, T>
{

    public SliceToEmbeddedIdBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator, Class<T> entityClass, EntityMeta meta,
            Object partitionKey, Object[] clusteringsTo)
    {
        super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
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
    public DefaultQueryBuilder<CONTEXT, T> fromEmbeddedId(Object fromEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
        List<Object> clusteringFrom = components.subList(1, components.size());

        super.fromClusteringsInternal(clusteringFrom.toArray(new Object[clusteringFrom.size()]));

        return this;
    }
}
