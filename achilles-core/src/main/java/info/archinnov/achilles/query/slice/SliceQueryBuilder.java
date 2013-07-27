package info.archinnov.achilles.query.slice;

import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;
import java.util.Iterator;
import java.util.List;

/**
 * SliceQueryBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryBuilder<CONTEXT extends PersistenceContext, T> extends RootSliceQueryBuilder<CONTEXT, T>
{

    public SliceQueryBuilder(SliceQueryExecutor<CONTEXT> sliceQueryExecutor,
            CompoundKeyValidator compoundKeyValidator, Class<T> entityClass,
            EntityMeta meta)
    {
        super(sliceQueryExecutor, compoundKeyValidator, entityClass, meta);
    }

    /**
     * Query by partition key and clustering components<br/>
     * <br/>
     * 
     * @param partitionKey
     *            Partition key
     * @return ThriftShortcutQueryBuilder<T>
     */
    public SliceShortcutQueryBuilder partitionKey(Object partitionKey)
    {
        super.partitionKeyInternal(partitionKey);
        return new SliceShortcutQueryBuilder();
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
    public SliceFromEmbeddedIdBuilder fromEmbeddedId(Object fromEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromEmbeddedId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");
        List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
        List<Object> clusteringFrom = components.subList(1, components.size());

        super.partitionKeyInternal(components.get(0));
        this.fromClusteringsInternal(clusteringFrom.toArray(new Object[clusteringFrom.size()]));

        return new SliceFromEmbeddedIdBuilder();
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
    public SliceToEmbeddedIdBuilder toEmbeddedId(Object toEmbeddedId)
    {
        Class<?> embeddedIdClass = meta.getIdClass();
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "toEmbeddedId should be of type '"
                + embeddedIdClass.getCanonicalName() + "'");

        List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
        List<Object> clusteringTo = components.subList(1, components.size());

        super.partitionKeyInternal(components.get(0));
        this.toClusteringsInternal(clusteringTo.toArray(new Object[clusteringTo.size()]));

        return new SliceToEmbeddedIdBuilder();
    }

    public class SliceShortcutQueryBuilder {

        protected SliceShortcutQueryBuilder() {
        }

        /**
         * Set from clustering components<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            From clustering components
         * 
         * @return ThriftFromClusteringsBuilder<T>
         */
        public SliceFromClusteringsBuilder fromClusterings(Object... clusteringComponents)
        {
            SliceQueryBuilder.super.fromClusteringsInternal(clusteringComponents);
            return new SliceFromClusteringsBuilder();
        }

        /**
         * Set to clustering components<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            To clustering components
         * 
         * @return ThriftToClusteringsBuilder<T>
         */
        public SliceToClusteringsBuilder toClusterings(Object... clusteringComponents)
        {
            SliceQueryBuilder.super.toClusteringsInternal(clusteringComponents);
            return new SliceToClusteringsBuilder();
        }

        /**
         * Get first n matching entities<br/>
         * <br/>
         * 
         * @param n
         *            first n matching entities
         * 
         * @return list of found entities or empty list
         */
        public List<T> get(int n)
        {
            return SliceQueryBuilder.super.get(n);
        }

        /**
         * Get first matching entity, using ASCENDING order<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         * 
         * @return first matching entity, filtered by provided clustering components if any, or null if no matching
         *         entity
         *         is found
         */
        public T getFirstOccurence(Object... clusteringComponents)
        {
            return SliceQueryBuilder.super.getFirstOccurence(clusteringComponents);
        }

        /**
         * Get first n matching entities, using ASCENDING order<br/>
         * <br/>
         * 
         * @param n
         *            first n matching entities
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         * 
         * @return list of n first matching entities, filtered by provided clustering components if any, or empty list
         */
        public List<T> getFirst(int n, Object... clusteringComponents)
        {
            return SliceQueryBuilder.super.getFirst(n, clusteringComponents);
        }

        /**
         * Get last matching entity, using ASCENDING order<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         * 
         * @return last matching entity, filtered by provided clustering components if any, or null if no matching
         *         entity
         *         is found
         */
        public T getLastOccurence(Object... clusteringComponents)
        {
            return SliceQueryBuilder.super.getLastOccurence(clusteringComponents);
        }

        /**
         * Get last n matching entities, using ASCENDING order<br/>
         * <br/>
         * 
         * @param n
         *            last n matching entities
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         * 
         * @return list of last n matching entities, filtered by provided clustering components if any, or empty list
         */
        public List<T> getLast(int n, Object... clusteringComponents)
        {
            return SliceQueryBuilder.super.getLast(n, clusteringComponents);
        }

        /**
         * Get entities iterator, using ASCENDING order<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         * 
         * @return iterator on found entities
         */
        public Iterator<T> iterator(Object... clusteringComponents)
        {
            return SliceQueryBuilder.super.iteratorWithComponents(clusteringComponents);
        }

        /**
         * Get entities iterator, using ASCENDING order<br/>
         * <br/>
         * 
         * @param batchSize
         *            batch loading size for iterator
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         * 
         * @return iterator on found entities
         */
        public Iterator<T> iterator(int batchSize, Object... clusteringComponents)
        {
            return SliceQueryBuilder.super.iteratorWithComponents(batchSize, clusteringComponents);
        }

        /**
         * Remove first n entities, using ASCENDING order<br/>
         * <br/>
         * 
         * @param n
         *            first n entities
         */
        public void remove(int n)
        {
            SliceQueryBuilder.super.remove(n);
        }

        /**
         * Remove first matching entity, using ASCENDING order<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         */
        public void removeFirstOccurence(Object... clusteringComponents)
        {
            SliceQueryBuilder.super.removeFirstOccurence(clusteringComponents);
        }

        /**
         * Remove first n matching entities, using ASCENDING order<br/>
         * <br/>
         * 
         * @param n
         *            first n matching entities
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         */
        public void removeFirst(int n, Object... clusteringComponents)
        {
            SliceQueryBuilder.super.removeFirst(n, clusteringComponents);
        }

        /**
         * Remove last matching entity, using ASCENDING order<br/>
         * <br/>
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         */
        public void removeLastOccurence(Object... clusteringComponents)
        {
            SliceQueryBuilder.super.removeLastOccurence(clusteringComponents);
        }

        /**
         * Remove last n matching entities, using ASCENDING order<br/>
         * <br/>
         * 
         * @param n
         *            last n matching entities
         * 
         * @param clusteringComponents
         *            optional clustering components for filtering
         */
        public void removeLast(int n, Object... clusteringComponents)
        {
            SliceQueryBuilder.super.removeLast(n, clusteringComponents);
        }
    }

    public class SliceFromEmbeddedIdBuilder
    {
        protected SliceFromEmbeddedIdBuilder() {
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
        public DefaultQueryBuilder toEmbeddedId(Object toEmbeddedId)
        {
            Class<?> embeddedIdClass = meta.getIdClass();
            PropertyMeta<?, ?> idMeta = meta.getIdMeta();
            Validator.validateInstanceOf(toEmbeddedId, embeddedIdClass, "toEmbeddedId should be of type '"
                    + embeddedIdClass.getCanonicalName() + "'");

            List<Object> components = idMeta.encodeToComponents(toEmbeddedId);
            List<Object> clusteringTo = components.subList(1, components.size());

            SliceQueryBuilder.super.toClusteringsInternal(clusteringTo.toArray(new Object[clusteringTo.size()]));

            return new DefaultQueryBuilder();
        }
    }

    public class SliceToEmbeddedIdBuilder
    {
        protected SliceToEmbeddedIdBuilder() {
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
        public DefaultQueryBuilder fromEmbeddedId(Object fromEmbeddedId)
        {
            Class<?> embeddedIdClass = meta.getIdClass();
            PropertyMeta<?, ?> idMeta = meta.getIdMeta();
            Validator.validateInstanceOf(fromEmbeddedId, embeddedIdClass, "fromEmbeddedId should be of type '"
                    + embeddedIdClass.getCanonicalName() + "'");

            List<Object> components = idMeta.encodeToComponents(fromEmbeddedId);
            List<Object> clusteringFrom = components.subList(1, components.size());

            SliceQueryBuilder.super
                    .fromClusteringsInternal(clusteringFrom.toArray(new Object[clusteringFrom.size()]));

            return new DefaultQueryBuilder();
        }
    }

    public class SliceFromClusteringsBuilder extends DefaultQueryBuilder {

        public SliceFromClusteringsBuilder() {
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
        public DefaultQueryBuilder toClusterings(Object... clusteringComponents)
        {
            SliceQueryBuilder.super.toClusteringsInternal(clusteringComponents);
            return new DefaultQueryBuilder();
        }
    }

    public class SliceToClusteringsBuilder extends DefaultQueryBuilder {

        public SliceToClusteringsBuilder() {
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
        public DefaultQueryBuilder fromClusterings(Object... clusteringComponents)
        {
            SliceQueryBuilder.super.fromClusteringsInternal(clusteringComponents);
            return new DefaultQueryBuilder();
        }
    }

    public class DefaultQueryBuilder {

        protected DefaultQueryBuilder() {
        }

        /**
         * Set ordering<br/>
         * <br/>
         * 
         * @param ordering
         *            ordering mode: ASCENDING or DESCENDING
         * 
         * @return DefaultQueryBuilder<T>
         */
        public DefaultQueryBuilder ordering(OrderingMode ordering) {
            SliceQueryBuilder.super.ordering(ordering);
            return this;
        }

        /**
         * Set bounding mode<br/>
         * <br/>
         * 
         * @param boundingMode
         *            bounding mode: ASCENDING or DESCENDING
         * 
         * @return DefaultQueryBuilder<T>
         */
        public DefaultQueryBuilder bounding(BoundingMode boundingMode)
        {
            SliceQueryBuilder.super.bounding(boundingMode);
            return this;
        }

        public DefaultQueryBuilder consistencyLevel(ConsistencyLevel consistencyLevel)
        {
            SliceQueryBuilder.super.consistencyLevel(consistencyLevel);
            return this;
        }

        public DefaultQueryBuilder limit(int limit)
        {
            SliceQueryBuilder.super.limit(limit);
            return this;
        }

        public List<T> get()
        {
            return SliceQueryBuilder.super.get();
        }

        public List<T> get(int n)
        {
            return SliceQueryBuilder.super.get(n);
        }

        public Iterator<T> iterator()
        {
            return SliceQueryBuilder.super.iterator();
        }

        public Iterator<T> iterator(int batchSize)
        {
            return SliceQueryBuilder.super.iterator(batchSize);
        }

        public void remove()
        {
            SliceQueryBuilder.super.remove();
        }

        public void remove(int n)
        {
            SliceQueryBuilder.super.remove(n);
        }
    }
}
