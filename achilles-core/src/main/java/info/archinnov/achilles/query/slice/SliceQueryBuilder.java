package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.QueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * SliceQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryBuilder<T>
{
    private QueryExecutor queryExecutor;
    private Class<T> entityClass;
    private EntityMeta meta;

    private CompoundKeyValidator compoundKeyValidator;

    public SliceQueryBuilder(QueryExecutor queryExecutor,
            CompoundKeyValidator compoundKeyValidator, Class<T> entityClass,
            EntityMeta meta)
    {
        this.queryExecutor = queryExecutor;
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
    public SliceShortcutQueryBuilder<T> partitionKey(Object partitionKey)
    {
        return new SliceShortcutQueryBuilder<T>(queryExecutor, compoundKeyValidator, entityClass, meta,
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
    public SliceFromEmbeddedIdBuilder<T> fromEmbeddedId(Object fromEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");
        List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
        List<Object> clusteringFrom = components.subList(1, components.size());

        return new SliceFromEmbeddedIdBuilder<T>(queryExecutor, compoundKeyValidator, entityClass, meta,
                components.get(0), clusteringFrom.toArray(new Object[clusteringFrom.size()]));
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
    public SliceToEmbeddedIdBuilder<T> toEmbeddedId(Object toEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
        List<Object> clusteringTo = components.subList(1, components.size());

        return new SliceToEmbeddedIdBuilder<T>(queryExecutor, compoundKeyValidator, entityClass, meta,
                components.get(0), clusteringTo.toArray(new Object[clusteringTo.size()]));
    }
}
