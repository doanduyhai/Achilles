package info.archinnov.achilles.query.slice;

import static info.archinnov.achilles.consistency.CQLConsistencyConvertor.getCQLLevel;
import info.archinnov.achilles.compound.CQLCompoundKeyValidator;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.query.SliceQuery;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.utils.Pair;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * CQLSliceQuery
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLSliceQuery<T> {

    private SliceQuery<T> sliceQuery;
    private List<Object> fixedComponents;
    private Object lastStartComp;
    private Object lastEndComp;

    private CQLCompoundKeyValidator validator = new CQLCompoundKeyValidator();

    public CQLSliceQuery(SliceQuery<T> sliceQuery) {
        this.sliceQuery = sliceQuery;
        this.fixedComponents = determineFixedComponents(sliceQuery);
        Pair<Object, Object> lastComponents = determineLastComponents(sliceQuery);
        this.lastStartComp = lastComponents.left;
        this.lastEndComp = lastComponents.right;
    }

    public List<Object> getFixedComponents()
    {
        return fixedComponents;
    }

    public Object getLastStartComponent() {
        return lastStartComp;
    }

    public Object getLastEndComponent() {
        return lastEndComp;
    }

    public int getLimit() {
        return sliceQuery.getLimit();
    }

    public ConsistencyLevel getConsistencyLevel() {
        return getCQLLevel(sliceQuery.getConsistencyLevel());
    }

    public BoundingMode getBounding() {
        return sliceQuery.getBounding();
    }

    public Ordering getOrdering()
    {
        OrderingMode ordering = sliceQuery.getOrdering();
        String orderingComponentName = sliceQuery.getMeta().getIdMeta().getOrderingComponent();
        if (ordering.isReverse())
        {
            return QueryBuilder.desc(orderingComponentName);
        }
        else
        {

            return QueryBuilder.asc(orderingComponentName);
        }
    }

    public List<String> getComponentNames() {
        return sliceQuery.getMeta().getIdMeta().getComponentNames();
    }

    public EntityMeta getMeta()
    {
        return sliceQuery.getMeta();
    }

    private List<Object> determineFixedComponents(SliceQuery<T> sliceQuery)
    {
        List<Object> fixedComponents = new ArrayList<Object>();

        List<Object> startComponents = sliceQuery.getClusteringsFrom();
        List<Object> endComponents = sliceQuery.getClusteringsTo();

        int startIndex = validator.getLastNonNullIndex(startComponents);
        int endIndex = validator.getLastNonNullIndex(endComponents);

        int minIndex = Math.min(startIndex, endIndex);

        if (startIndex == endIndex)
        {
            for (int i = 0; i <= minIndex && startComponents.get(i).equals(endComponents.get(i)); i++)
            {
                fixedComponents.add(startComponents.get(i));
            }
        }
        else
        {
            for (int i = 0; i <= minIndex; i++)
            {
                fixedComponents.add(startComponents.get(i));
            }
        }

        return fixedComponents;
    }

    private Pair<Object, Object> determineLastComponents(SliceQuery<T> sliceQuery)
    {
        Object lastStartComp;
        Object lastEndComp;

        List<Object> startComponents = sliceQuery.getClusteringsFrom();
        List<Object> endComponents = sliceQuery.getClusteringsTo();

        int startIndex = validator.getLastNonNullIndex(startComponents);
        int endIndex = validator.getLastNonNullIndex(endComponents);

        if (startIndex == endIndex && !startComponents.get(startIndex).equals(endComponents.get(endIndex)))
        {
            lastStartComp = startComponents.get(startIndex);
            lastEndComp = endComponents.get(endIndex);
        }
        else if (startIndex < endIndex)
        {
            lastStartComp = null;
            lastEndComp = endComponents.get(endIndex);
        }
        else if (startIndex > endIndex)
        {
            lastStartComp = startComponents.get(startIndex);
            lastEndComp = null;
        }
        else
        {
            lastStartComp = null;
            lastEndComp = null;
        }

        return Pair.create(lastStartComp, lastEndComp);
    }
}
