package info.archinnov.achilles.iterator;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.CQLEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.CQLEntityProxifier;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import java.util.Iterator;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;

/**
 * CQLSliceQueryIterator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLSliceQueryIterator<T> implements Iterator<T> {

    private Class<T> entityClass;
    private CQLPersistenceContext context;
    private Iterator<Row> iterator;
    private String varyingComponentName;
    private Object lastVaryingComponentValue;
    private Class<?> varyingComponentClass;
    private PreparedStatement ps;
    private EntityMeta meta;
    private int batchSize;
    private int count = 0;

    private ReflectionInvoker invoker = new ReflectionInvoker();
    private CQLEntityMapper mapper = new CQLEntityMapper();
    private CQLRowMethodInvoker cqlInvoker = new CQLRowMethodInvoker();
    private CQLEntityProxifier proxifier = new CQLEntityProxifier();

    public CQLSliceQueryIterator(CQLSliceQuery<T> sliceQuery, CQLPersistenceContext context, Iterator<Row> iterator,
            PreparedStatement ps) {
        this.context = context;
        this.iterator = iterator;
        this.ps = ps;

        this.entityClass = sliceQuery.getEntityClass();
        this.meta = sliceQuery.getMeta();
        this.varyingComponentName = sliceQuery.getVaryingComponentName();
        this.varyingComponentClass = sliceQuery.getVaryingComponentClass();
        this.batchSize = sliceQuery.getBatchSize();
    }

    @Override
    public boolean hasNext() {
        if (!iterator.hasNext() && count == batchSize)
        {
            iterator = context.bindAndExecute(ps, lastVaryingComponentValue).iterator();
            count = 0;
        }
        return iterator.hasNext();
    }

    @Override
    public T next() {

        Row row = iterator.next();
        lastVaryingComponentValue = cqlInvoker.invokeOnRowForType(row, varyingComponentClass,
                varyingComponentName);
        T clusteredEntity = invoker.instanciate(entityClass);
        mapper.setEagerPropertiesToEntity(row, meta, clusteredEntity);
        count++;
        return proxify(clusteredEntity);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove clustered entity with iterator");
    }

    private T proxify(T clusteredEntity)
    {
        CQLPersistenceContext duplicate = context.duplicate(clusteredEntity);
        return proxifier.buildProxy(clusteredEntity, duplicate);
    }

}
