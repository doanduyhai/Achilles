package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import java.util.List;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.FluentIterable;

/**
 * CQLStringStatementGenerator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLStatementGenerator {

    private SliceQueryStatementGenerator sliceQueryGenerator = new SliceQueryStatementGenerator();

    public <T> Query generateSliceQuery(CQLSliceQuery<T> sliceQuery)
    {
        EntityMeta meta = sliceQuery.getMeta();

        Select select = generateSelectEntity(meta);
        select = select.limit(sliceQuery.getLimit());
        select.orderBy(sliceQuery.getOrdering());

        Statement where = sliceQueryGenerator.generateWhereClauseForSliceQuery(sliceQuery, select);

        return where.setConsistencyLevel(sliceQuery.getConsistencyLevel());

    }

    public Select generateSelectEntity(EntityMeta entityMeta) {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

        Selection select = select();

        generateSelectForPrimaryKey(idMeta, select);

        List<PropertyMeta<?, ?>> eagerMetas = FluentIterable
                .from(entityMeta.getEagerMetas())
                .filter(PropertyType.excludeIdType)
                .toImmutableList();

        for (PropertyMeta<?, ?> pm : eagerMetas) {
            select.column(pm.getPropertyName());
        }
        return select.from(entityMeta.getTableName());
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

}
