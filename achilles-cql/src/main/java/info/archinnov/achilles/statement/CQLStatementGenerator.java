package info.archinnov.achilles.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.statement.prepared.CQLSliceQueryPreparedStatementGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.collect.FluentIterable;

/**
 * CQLStringStatementGenerator
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLStatementGenerator {

    private ReflectionInvoker invoker = new ReflectionInvoker();
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

    public Insert generateInsert(Object entity, EntityMeta entityMeta)
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        Insert insert = insertInto(entityMeta.getTableName());
        generateInsertPrimaryKey(entity, idMeta, insert);

        List<PropertyMeta<?, ?>> nonProxyMetas = FluentIterable
                .from(entityMeta.getAllMetasExceptIdMeta())
                .filter(PropertyType.excludeCounterType)
                .toImmutableList();

        List<PropertyMeta<?, ?>> fieldMetas = new ArrayList<PropertyMeta<?, ?>>(nonProxyMetas);
        fieldMetas.remove(idMeta);

        for (PropertyMeta<?, ?> pm : fieldMetas)
        {
            Object value = invoker.getValueFromField(entity, pm.getGetter());
            value = encodeValueForCassandra(pm, value);
            insert.value(pm.getPropertyName(), value);
        }
        return insert;
    }

    public Update.Assignments generateUpdateFields(Object entity, EntityMeta entityMeta,
            List<PropertyMeta<?, ?>> pms)
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        Update update = update(entityMeta.getTableName());

        int i = 0;
        Assignments assignments = null;
        for (PropertyMeta<?, ?> pm : pms)
        {
            Object value = invoker.getValueFromField(entity, pm.getGetter());
            value = encodeValueForCassandra(pm, value);
            if (i == 0)
            {
                assignments = update.with(set(pm.getPropertyName(), value));
            }
            else
            {
                assignments.and(set(pm.getPropertyName(), value));
            }
            i++;
        }
        return generateWhereClauseForUpdate(entity, idMeta, assignments);
    }

    private Update.Assignments generateWhereClauseForUpdate(Object entity, PropertyMeta<?, ?> idMeta,
            Assignments update)
    {
        if (idMeta.isEmbeddedId())
        {
            Update.Where where = null;
            int index = 0;

            Object primaryKey = invoker.getValueFromField(entity, idMeta.getGetter());
            List<String> componentNames = idMeta.getComponentNames();
            List<Object> encodedComponents = idMeta.encodeToComponents(primaryKey);
            for (int i = 0; i < encodedComponents.size(); i++)
            {
                String componentName = componentNames.get(i);
                Object componentValue = encodedComponents.get(i);
                if (index == 0)
                {
                    where = update.where(eq(componentName, componentValue));
                }
                else
                {
                    where.and(eq(componentName, componentValue));
                }
                index++;
            }
        }
        else
        {
            Object id = invoker.getValueFromField(entity, idMeta.getGetter());
            update.where(eq(idMeta.getPropertyName(), id));
        }
        return update;
    }

    private void generateInsertPrimaryKey(Object entity, PropertyMeta<?, ?> idMeta, Insert insert)
    {
        Object primaryKey = invoker.getValueFromField(entity, idMeta.getGetter());
        if (idMeta.isEmbeddedId())
        {
            List<String> componentNames = idMeta.getComponentNames();
            List<Object> encodedComponents = idMeta.encodeToComponents(primaryKey);
            for (int i = 0; i < encodedComponents.size(); i++)
            {
                String componentName = componentNames.get(i);
                Object componentValue = encodedComponents.get(i);
                insert.value(componentName, componentValue);
            }
        }
        else
        {
            Object id = idMeta.encode(primaryKey);
            insert.value(idMeta.getPropertyName(), id);
        }
    }

    private Object encodeValueForCassandra(PropertyMeta<?, ?> pm, Object value)
    {
        if (value != null)
        {
            switch (pm.type())
            {
                case SIMPLE:
                case LAZY_SIMPLE:
                case JOIN_SIMPLE:
                    return pm.encode(value);
                case LIST:
                case LAZY_LIST:
                case JOIN_LIST:
                    return pm.encode((List) value);
                case SET:
                case LAZY_SET:
                case JOIN_SET:
                    return pm.encode((Set) value);
                case MAP:
                case LAZY_MAP:
                case JOIN_MAP:
                    return pm.encode((Map) value);
                default:
                    throw new AchillesException("Cannot encode value '" + value + "' for Cassandra for property '"
                            + pm.getPropertyName() + "' of type '" + pm.type().name() + "'");
            }
        }
        return value;
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
