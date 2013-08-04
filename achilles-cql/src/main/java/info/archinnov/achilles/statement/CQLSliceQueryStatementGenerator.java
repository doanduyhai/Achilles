package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.type.OrderingMode;
import java.util.List;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

/**
 * CQLSliceQueryStatementGenerator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLSliceQueryStatementGenerator {

    public <T> Statement generateWhereClauseForSelectSliceQuery(CQLSliceQuery<T> sliceQuery, Select select) {

        Where where = select.where();
        List<Object> fixedComponents = sliceQuery.getFixedComponents();
        List<String> componentNames = sliceQuery.getComponentNames();
        String varyingComponentName = sliceQuery.getVaryingComponentName();
        OrderingMode ordering = sliceQuery.getAchillesOrdering();

        Object lastStartComp = sliceQuery.getLastStartComponent();
        Object lastEndComp = sliceQuery.getLastEndComponent();

        for (int i = 0; i < fixedComponents.size(); i++)
        {
            where.and(eq(componentNames.get(i), fixedComponents.get(i)));
        }

        if (ordering == ASCENDING)
        {

            switch (sliceQuery.getBounding()) {
                case INCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(gte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lte(varyingComponentName, lastEndComp));
                    break;
                case EXCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(gt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_START_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(gte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_END_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(gt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lte(varyingComponentName, lastEndComp));
                    break;
            }
        }
        else // ordering == DESCENDING
        {
            switch (sliceQuery.getBounding()) {
                case INCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(lte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gte(varyingComponentName, lastEndComp));
                    break;
                case EXCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(lt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_START_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(lte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_END_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(lt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gte(varyingComponentName, lastEndComp));
                    break;
            }

        }
        return where;
    }

    public <T> Statement generateWhereClauseForDeleteSliceQuery(CQLSliceQuery<T> sliceQuery, Delete delete)
    {
        List<Object> fixedComponents = sliceQuery.getFixedComponents();
        List<String> componentNames = sliceQuery.getComponentNames();

        com.datastax.driver.core.querybuilder.Delete.Where where = delete.where();

        for (int i = 0; i < fixedComponents.size(); i++)
        {
            where.and(eq(componentNames.get(i), fixedComponents.get(i)));
        }
        return where;
    }

}
