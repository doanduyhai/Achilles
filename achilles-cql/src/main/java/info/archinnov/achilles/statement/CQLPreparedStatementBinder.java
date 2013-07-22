package info.archinnov.achilles.statement;

import info.archinnov.achilles.compound.CQLCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import java.util.ArrayList;
import java.util.List;
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
    private CQLCompoundKeyMapper mapper = new CQLCompoundKeyMapper();

    public BoundStatement bindForInsert(PreparedStatement ps, EntityMeta entityMeta, Object entity)
    {
        List<Object> values = new ArrayList<Object>();
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        Object primaryKey = invoker.getValueFromField(entity, idMeta.getGetter());
        values.addAll(bindPrimaryKey(primaryKey, idMeta));

        List<PropertyMeta<?, ?>> nonProxyMetas = FluentIterable
                .from(entityMeta.getAllMetasExceptIdMeta())
                .filter(PropertyType.excludeProxyType)
                .toImmutableList();

        List<PropertyMeta<?, ?>> fieldMetas = new ArrayList<PropertyMeta<?, ?>>(nonProxyMetas);
        fieldMetas.remove(idMeta);

        for (PropertyMeta<?, ?> pm : fieldMetas)
        {
            Object value = invoker.getValueFromField(entity, pm.getGetter());
            value = extractFieldFromEntity(pm, value);
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
            value = extractFieldFromEntity(pm, value);
            values.add(value);
        }
        Object primaryKey = invoker.getValueFromField(entity, idMeta.getGetter());
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
            values.addAll(mapper.extractComponents(primaryKey, idMeta));
        }
        else
        {
            values.add(primaryKey);
        }
        return values;
    }

    private Object extractFieldFromEntity(PropertyMeta<?, ?> pm, Object value)
    {
        if (value != null)
        {
            if (pm.isJoin())
            {
                value = invoker.getPrimaryKey(value, pm.joinIdMeta());
            }
            else
            {
                value = pm.writeValueAsSupportedTypeOrString(value);
            }
        }
        return value;
    }

    private Object[] extractValuesForSimpleCounterBinding(EntityMeta entityMeta,
            PropertyMeta<?, ?> pm, Object primaryKey)
    {
        PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();
        String fqcn = entityMeta.getClassName();
        String primaryKeyAsString = idMeta.jsonSerializeValue(primaryKey);
        String propertyName = pm.getCQLPropertyName();

        return new Object[]
        {
                fqcn,
                primaryKeyAsString,
                propertyName
        };
    }
}
