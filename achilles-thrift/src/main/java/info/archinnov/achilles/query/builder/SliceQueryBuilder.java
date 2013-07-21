package info.archinnov.achilles.query.builder;

import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftQueryExecutor;
import info.archinnov.achilles.validation.Validator;
import java.util.List;

/**
 * RootQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryBuilder<T>
{
    private ThriftQueryExecutor queryExecutor;
    private Class<T> entityClass;
    private EntityMeta meta;

    private ThriftCompoundKeyMapper mapper = new ThriftCompoundKeyMapper();

    public SliceQueryBuilder(ThriftQueryExecutor queryExecutor, Class<T> entityClass,
            EntityMeta meta)
    {
        this.queryExecutor = queryExecutor;
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
    public ThriftShortcutQueryBuilder<T> partitionKey(Object partitionKey)
    {
        return new ThriftShortcutQueryBuilder<T>(queryExecutor, entityClass, meta,
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
    public ThriftFromEmbeddedIdBuilder<T> fromEmbeddedId(Object fromEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = mapper.fromCompoundToComponents(fromEmbeddedId,
                idMeta.getComponentGetters());
        List<Object> clusteringFrom = components.subList(1, components.size());

        return new ThriftFromEmbeddedIdBuilder<T>(queryExecutor, entityClass, meta, components.get(0),
                clusteringFrom.toArray(new Object[clusteringFrom.size()]));
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
    public ThriftToEmbeddedIdBuilder<T> toEmbeddedId(Object toEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "fromId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = mapper.fromCompoundToComponents(toEmbeddedId,
                idMeta.getComponentGetters());
        List<Object> clusteringTo = components.subList(1, components.size());

        return new ThriftToEmbeddedIdBuilder<T>(queryExecutor, entityClass, meta, components.get(0),
                clusteringTo.toArray(new Object[clusteringTo.size()]));
    }
}
