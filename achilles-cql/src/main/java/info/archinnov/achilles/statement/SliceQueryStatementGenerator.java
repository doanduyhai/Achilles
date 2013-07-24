package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import java.util.List;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;

/**
 * SliceQueryStatementGenerator
 * 
 * @author DuyHai DOAN
 * 
 */
public class SliceQueryStatementGenerator {

    public <T> Statement generateWhereClauseForSliceQuery(CQLSliceQuery<T> sliceQuery, Select select) {

        Where where = select.where();
        List<Object> fixedComponents = sliceQuery.getFixedComponents();
        List<String> componentNames = sliceQuery.getComponentNames();
        String varyingComponentName = componentNames.get(fixedComponents.size());

        Object lastStartComp = sliceQuery.getLastStartComponent();
        Object lastEndComp = sliceQuery.getLastEndComponent();

        for (int i = 0; i < fixedComponents.size(); i++)
        {
            where.and(eq(componentNames.get(i), fixedComponents.get(i)));
        }

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
        return where;
    }

}
