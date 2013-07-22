package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.query.SliceQueryValidator;
import info.archinnov.achilles.type.BoundingMode;
import java.util.List;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;

/**
 * CQLStringStatementGenerator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLStatementGenerator {
    private SliceQueryValidator validator = new SliceQueryValidator();

    public Select generateSelectEntity(EntityMeta entityMeta) {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

        Selection select = select();

        generateSelectForPrimaryKey(idMeta, select);

        for (PropertyMeta<?, ?> pm : entityMeta.getEagerMetas()) {
            select.column(pm.getPropertyName());
        }
        return select.from(entityMeta.getCQLTableName());

    }

    private void generateSelectForPrimaryKey(PropertyMeta<?, ?> idMeta, Selection select) {
        if (idMeta.isCompound()) {
            for (String component : idMeta.getComponentNames()) {
                select.column(component);
            }
        } else {
            select.column(idMeta.getPropertyName());
        }
    }

    public Statement generateWhereClauseForSliceQuery(List<String> componentNames, List<Comparable<?>> startValues,
            List<Comparable<?>> endValues, BoundingMode boundingMode, Select select) {
        int startIndex = validator.findLastNonNullIndexForComponents(startValues);
        int endIndex = validator.findLastNonNullIndexForComponents(endValues);

        Where where = select.where();
        if (startIndex == endIndex) {
            for (int i = 0; i < startIndex; i++) {
                where.and(eq(componentNames.get(i), startValues.get(i)));
            }
            buildWhereClauseForLastComponents(componentNames, startValues, endValues, boundingMode, startIndex, where);

        } else {
            for (int i = 0; i <= Math.min(startIndex, endIndex); i++) {
                where.and(eq(componentNames.get(i), startValues.get(i)));
            }
            buildWhereClauseForLastNonNullComponent(componentNames, startValues, endValues, boundingMode, startIndex,
                    endIndex, where);
        }
        return where;
    }

    private void buildWhereClauseForLastComponents(List<String> componentNames, List<Comparable<?>> startValues,
            List<Comparable<?>> endValues, BoundingMode boundingMode, int startIndex, Where where) {
        switch (boundingMode) {
            case INCLUSIVE_BOUNDS:
                where.and(gte(componentNames.get(startIndex), startValues.get(startIndex)));
                where.and(lte(componentNames.get(startIndex), endValues.get(startIndex)));
                break;
            case EXCLUSIVE_BOUNDS:
                where.and(gt(componentNames.get(startIndex), startValues.get(startIndex)));
                where.and(lt(componentNames.get(startIndex), endValues.get(startIndex)));
                break;
            case INCLUSIVE_START_BOUND_ONLY:
                where.and(gte(componentNames.get(startIndex), startValues.get(startIndex)));
                where.and(lt(componentNames.get(startIndex), endValues.get(startIndex)));
                break;
            case INCLUSIVE_END_BOUND_ONLY:
                where.and(gt(componentNames.get(startIndex), startValues.get(startIndex)));
                where.and(lte(componentNames.get(startIndex), endValues.get(startIndex)));
                break;
        }
    }

    private void buildWhereClauseForLastNonNullComponent(List<String> componentNames,
            List<Comparable<?>> startValues,
            List<Comparable<?>> endValues, BoundingMode boundingMode, int startIndex, int endIndex, Where where) {
        if (startIndex > endIndex) {
            switch (boundingMode) {
                case INCLUSIVE_BOUNDS:
                case INCLUSIVE_START_BOUND_ONLY:
                    where.and(gte(componentNames.get(startIndex), startValues.get(startIndex)));
                    break;
                case EXCLUSIVE_BOUNDS:
                case INCLUSIVE_END_BOUND_ONLY:
                    where.and(gt(componentNames.get(startIndex), startValues.get(startIndex)));
                    break;
            }
        } else {
            switch (boundingMode) {
                case INCLUSIVE_BOUNDS:
                case INCLUSIVE_END_BOUND_ONLY:
                    where.and(lte(componentNames.get(endIndex), endValues.get(endIndex)));
                    break;
                case EXCLUSIVE_BOUNDS:
                case INCLUSIVE_START_BOUND_ONLY:
                    where.and(lt(componentNames.get(endIndex), endValues.get(endIndex)));
                    break;
            }
        }
    }
}
