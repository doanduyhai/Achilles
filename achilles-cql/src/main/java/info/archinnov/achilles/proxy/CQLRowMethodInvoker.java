package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.cql.CQLTypeMapper.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.datastax.driver.core.Row;

/**
 * CQLRowMethodInvoker
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLRowMethodInvoker
{
    public Object invokeOnRowForFields(Row row, PropertyMeta<?, ?> pm)
    {
        String propertyName = pm.getCQLPropertyName();
        Object value = null;
        if (row != null && !row.isNull(propertyName))
        {
            switch (pm.type())
            {
                case LIST:
                case LAZY_LIST:
                    value = invokeOnRowForList(row, pm, propertyName, pm.getValueClass());
                    break;
                case SET:
                case LAZY_SET:
                    value = invokeOnRowForSet(row, pm, propertyName, pm.getValueClass());
                    break;
                case MAP:
                case LAZY_MAP:
                    Class<?> keyClass = pm.getKeyClass();
                    Class<?> valueClass = pm.getValueClass();
                    value = invokeOnRowForMap(row, pm, propertyName, keyClass, valueClass);
                    break;
                case ID:
                case SIMPLE:
                case LAZY_SIMPLE:
                    value = invokeOnRowForProperty(row, pm, propertyName, pm.getValueClass());
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    public Object invokeOnRowForClusteredComponent(Row row, PropertyMeta<?, ?> pm, String componentName,
            Class<?> valueClass)
    {
        try
        {
            Object rawValue = getRowMethod(valueClass).invoke(row, componentName);
            return pm.convertValueFromCassandra(valueClass, rawValue);
        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve property '" + componentName
                    + "' for entity class '" + pm.getEntityClassName() + "' from CQL Row", e);
        }
    }

    public Object invokeOnRowForProperty(Row row, PropertyMeta<?, ?> pm, String propertyName, Class<?> valueClass)
    {
        try
        {
            Object rawValue = getRowMethod(valueClass).invoke(row, propertyName);
            return pm.getValueFromCassandra(rawValue);
        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve property '" + propertyName
                    + "' for entity class '" + pm.getEntityClassName() + "' from CQL Row", e);
        }
    }

    public List<?> invokeOnRowForList(Row row, PropertyMeta<?, ?> pm, String propertyName, Class<?> valueClass)
    {
        try {
            Collection<?> rawValues = row.getList(propertyName, toCompatibleJavaType(valueClass));
            return new ArrayList<Object>(pm.getValuesFromCassandra(rawValues));

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve list property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Set<?> invokeOnRowForSet(Row row, PropertyMeta<?, ?> pm, String propertyName, Class<?> valueClass)
    {
        try {
            Collection<?> rawValues = row.getSet(propertyName, toCompatibleJavaType(valueClass));
            return new HashSet<Object>(pm.getValuesFromCassandra(rawValues));

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve set property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Map<?, ?> invokeOnRowForMap(Row row, PropertyMeta<?, ?> pm, String propertyName, Class<?> keyClass,
            Class<?> valueClass)
    {
        try
        {
            Map<?, ?> rawValues = row.getMap(propertyName, toCompatibleJavaType(keyClass),
                    toCompatibleJavaType(valueClass));
            return pm.getValuesFromCassandra(rawValues);

        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve map property '" + propertyName
                    + "' from CQL Row", e);
        }
    }
}
