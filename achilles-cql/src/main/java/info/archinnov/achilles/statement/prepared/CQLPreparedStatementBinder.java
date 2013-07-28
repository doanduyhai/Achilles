package info.archinnov.achilles.statement.prepared;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.FluentIterable;

/**
 * CQLPreparedStatementBinder
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLPreparedStatementBinder
{
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public BoundStatement bindForInsert(PreparedStatement ps, EntityMeta entityMeta, Object entity)
    {
        List<Object> values = new ArrayList<Object>();
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        Object primaryKey = invoker.getPrimaryKey(entity, idMeta);
        values.addAll(bindPrimaryKey(primaryKey, idMeta));

        List<PropertyMeta<?, ?>> nonProxyMetas = FluentIterable
                .from(entityMeta.getAllMetasExceptIdMeta())
                .filter(PropertyType.excludeCounterType)
                .toImmutableList();

        List<PropertyMeta<?, ?>> fieldMetas = new ArrayList<PropertyMeta<?, ?>>(nonProxyMetas);

        for (PropertyMeta<?, ?> pm : fieldMetas)
        {
            Object value = invoker.getValueFromField(entity, pm.getGetter());
            value = encodeValueForCassandra(pm, value);
            values.add(value);
        }
        return ps.bind(values.toArray(new Object[values.size()]));
    }

    public BoundStatement bindForUpdate(PreparedStatement ps, EntityMeta entityMeta,
            List<PropertyMeta<?, ?>> pms, Object entity)
    {
        List<Object> values = new ArrayList<Object>();
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        for (PropertyMeta<?, ?> pm : pms)
        {
            Object value = invoker.getValueFromField(entity, pm.getGetter());
            value = encodeValueForCassandra(pm, value);
            values.add(value);
        }
        Object primaryKey = invoker.getPrimaryKey(entity, idMeta);
        values.addAll(bindPrimaryKey(primaryKey, idMeta));
        return ps.bind(values.toArray(new Object[values.size()]));
    }

    public BoundStatement bindStatementWithOnlyPKInWhereClause(PreparedStatement ps,
            EntityMeta entityMeta, Object primaryKey)
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        List<Object> values = bindPrimaryKey(primaryKey, idMeta);
        return ps.bind(values.toArray(new Object[values.size()]));
    }

    public BoundStatement bindForSimpleCounterIncrementDecrement(PreparedStatement ps,
            EntityMeta entityMeta, PropertyMeta<?, ?> pm, Object primaryKey, Long increment)
    {
        Object[] values = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
        return ps.bind(ArrayUtils.add(values, 0, increment));
    }

    public BoundStatement bindForSimpleCounterSelect(PreparedStatement ps, EntityMeta entityMeta,
            PropertyMeta<?, ?> pm, Object primaryKey)
    {
        Object[] values = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
        return ps.bind(values);
    }

    public BoundStatement bindForSimpleCounterDelete(PreparedStatement ps, EntityMeta entityMeta,
            PropertyMeta<?, ?> pm, Object primaryKey)
    {
        Object[] values = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
        return ps.bind(values);
    }

    private List<Object> bindPrimaryKey(Object primaryKey, PropertyMeta<?, ?> idMeta)
    {
        List<Object> values = new ArrayList<Object>();
        if (idMeta.isCompound())
        {
            values.addAll(idMeta.encodeToComponents(primaryKey));
        }
        else
        {
            values.add(idMeta.encode(primaryKey));
        }
        return values;
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

    private Object[] extractValuesForSimpleCounterBinding(EntityMeta entityMeta,
            PropertyMeta<?, ?> pm, Object primaryKey)
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        String fqcn = entityMeta.getClassName();
        String primaryKeyAsString = idMeta.forceEncodeToJSON(primaryKey);
        String propertyName = pm.getPropertyName();

        return new Object[]
        {
                fqcn,
                primaryKeyAsString,
                propertyName
        };
    }
}
