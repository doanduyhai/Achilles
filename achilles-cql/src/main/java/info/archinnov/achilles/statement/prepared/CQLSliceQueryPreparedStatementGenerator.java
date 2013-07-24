package info.archinnov.achilles.statement.prepared;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.type.OrderingMode;
import java.util.List;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

/**
 * CQLSliceQueryPreparedStatementGenerator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLSliceQueryPreparedStatementGenerator {

    public <T> Statement generateWhereClauseForIteratorSliceQuery(CQLSliceQuery<T> sliceQuery, Select select) {

        Where where = select.where();
        List<Object> fixedComponents = sliceQuery.getFixedComponents();
        List<String> componentNames = sliceQuery.getComponentNames();
        String varyingComponentName = componentNames.get(fixedComponents.size());
        OrderingMode ordering = sliceQuery.getAchillesOrdering();

        for (int i = 0; i < fixedComponents.size(); i++)
        {
            where.and(eq(componentNames.get(i), fixedComponents.get(i)));
        }

        Object lastEndComp = sliceQuery.getLastEndComponent();
        if (ordering == ASCENDING)
        {
            switch (sliceQuery.getBounding()) {
                case INCLUSIVE_BOUNDS:
                case INCLUSIVE_END_BOUND_ONLY:
                    where.and(gt(varyingComponentName, bindMarker()));
                    if (lastEndComp != null)
                        where.and(lte(varyingComponentName, lastEndComp));
                    break;
                case EXCLUSIVE_BOUNDS:
                case INCLUSIVE_START_BOUND_ONLY:
                    where.and(gt(varyingComponentName, bindMarker()));
                    if (lastEndComp != null)
                        where.and(lt(varyingComponentName, lastEndComp));
                    break;
            }
        }
        else // ordering == DESCENDING
        {
            switch (sliceQuery.getBounding()) {
                case INCLUSIVE_BOUNDS:
                case INCLUSIVE_END_BOUND_ONLY:
                    where.and(lt(varyingComponentName, bindMarker()));
                    if (lastEndComp != null)
                        where.and(gte(varyingComponentName, lastEndComp));
                    break;
                case EXCLUSIVE_BOUNDS:
                case INCLUSIVE_START_BOUND_ONLY:
                    where.and(lt(varyingComponentName, bindMarker()));
                    if (lastEndComp != null)
                        where.and(gt(varyingComponentName, lastEndComp));
                    break;
            }

        }
        return where;
    }
}
