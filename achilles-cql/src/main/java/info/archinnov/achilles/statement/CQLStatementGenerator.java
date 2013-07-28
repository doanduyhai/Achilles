package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.prepared.CQLSliceQueryPreparedStatementGenerator;
import java.util.List;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
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

    private CQLSliceQueryStatementGenerator sliceQueryGenerator = new CQLSliceQueryStatementGenerator();
    private CQLSliceQueryPreparedStatementGenerator sliceQueryPreparedGenerator = new CQLSliceQueryPreparedStatementGenerator();

    public <T> Query generateSelectSliceQuery(CQLSliceQuery<T> sliceQuery, int limit)
    {
        EntityMeta meta = sliceQuery.getMeta();

        Select select = generateSelectEntity(meta);
        select = select.limit(limit);
        select.orderBy(sliceQuery.getCQLOrdering());

        Statement where = sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(sliceQuery, select);

        return where.setConsistencyLevel(sliceQuery.getConsistencyLevel());
    }

    public <T> PreparedStatement generateIteratorSliceQuery(CQLSliceQuery<T> sliceQuery, CQLDaoContext daoContext)
    {
        EntityMeta meta = sliceQuery.getMeta();

        Select select = generateSelectEntity(meta);
        select = select.limit(sliceQuery.getLimit());
        select.orderBy(sliceQuery.getCQLOrdering());

        Statement where = sliceQueryPreparedGenerator.generateWhereClauseForIteratorSliceQuery(sliceQuery, select);

        PreparedStatement preparedStatement = daoContext.prepare(where);
        preparedStatement.setConsistencyLevel(sliceQuery.getConsistencyLevel());
        return preparedStatement;
    }

    public <T> Query generateRemoveSliceQuery(CQLSliceQuery<T> sliceQuery)
    {
        EntityMeta meta = sliceQuery.getMeta();

        Delete delete = QueryBuilder.delete().from(meta.getTableName());
        Statement where = sliceQueryGenerator.generateWhereClauseForDeleteSliceQuery(sliceQuery, delete);

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
        if (idMeta.isEmbeddedId()) {
            for (String component : idMeta.getComponentNames()) {
                select.column(component);
            }
        } else {
            select.column(idMeta.getPropertyName());
        }
    }

}
