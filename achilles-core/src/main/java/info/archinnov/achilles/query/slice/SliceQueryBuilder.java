package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * SliceQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryBuilder<CONTEXT extends PersistenceContext, T>
{
    private SliceQueryExecutor<CONTEXT> sliceQueryExecutor;
    private Class<T> entityClass;
    private EntityMeta meta;
    private CompoundKeyValidator compoundKeyValidator;

    public SliceQueryBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator, Class<T> entityClass,
            EntityMeta meta)
    {
        this.sliceQueryExecutor = sliceQueryExecutor;
        this.compoundKeyValidator = compoundKeyValidator;
        this.entityClass = entityClass;
        this.meta = meta;
    }

    /**
     * Query by partition key and clustering components<br/>
     * <br/>
     * 
     * @param partitionKey
     *            Partition key
     * @return ThriftShortcutQueryBuilder<T>
     */
    public SliceShortcutQueryBuilder<CONTEXT, T> partitionKey(Object partitionKey)
    {
        return new SliceShortcutQueryBuilder<CONTEXT, T>(sliceQueryExecutor, compoundKeyValidator, entityClass, meta,
                partitionKey);
    }

    /**
     * Query by from & to embeddedIds<br/>
     * <br/>
     * 
     * @param fromEmbeddedId
     *            From embeddedId
     * 
     * @return ThriftFromEmbeddedIdBuilder<T>
     */
    public SliceFromEmbeddedIdBuilder<CONTEXT, T> fromEmbeddedId(Object fromEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");
        List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
        List<Object> clusteringFrom = components.subList(1, components.size());

        return new SliceFromEmbeddedIdBuilder<CONTEXT, T>(sliceQueryExecutor, compoundKeyValidator, entityClass,
                meta, components.get(0), clusteringFrom.toArray(new Object[clusteringFrom.size()]));
    }

    /**
     * Query by from & to embeddedIds<br/>
     * <br/>
     * 
     * @param toEmbeddedId
     *            To embeddedId
     * 
     * @return ThriftToEmbeddedIdBuilder<T>
     */
    public SliceToEmbeddedIdBuilder<CONTEXT, T> toEmbeddedId(Object toEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
        List<Object> clusteringTo = components.subList(1, components.size());

        return new SliceToEmbeddedIdBuilder<CONTEXT, T>(sliceQueryExecutor, compoundKeyValidator, entityClass, meta,
                components.get(0), clusteringTo.toArray(new Object[clusteringTo.size()]));
    }
}
