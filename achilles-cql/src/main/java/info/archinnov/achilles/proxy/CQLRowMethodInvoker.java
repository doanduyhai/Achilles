package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.type.CQLTypeMapper.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
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
        String propertyName = pm.getPropertyName();
        Object value = null;
        if (row != null && !row.isNull(propertyName))
        {
            switch (pm.type())
            {
                case LIST:
                case LAZY_LIST:
                    value = invokeOnRowForList(row, pm.getPropertyName(), pm.getValueClass());
                    break;
                case SET:
                case LAZY_SET:
                    value = invokeOnRowForSet(row, pm.getPropertyName(), pm.getValueClass());
                    break;
                case MAP:
                case LAZY_MAP:
                    Class<?> keyClass = pm.getKeyClass();
                    Class<?> valueClass = pm.getValueClass();
                    value = invokeOnRowForMap(row, pm.getPropertyName(), keyClass, valueClass);
                    break;
                case ID:
                case SIMPLE:
                case LAZY_SIMPLE:
                    Object rawValue = invokeOnRowForProperty(row, pm.getPropertyName(), pm.getValueClass());
                    value = pm.getValueFromCassandra(rawValue);
                    break;
                default:
                    break;
            }
        }
        return value;
    }

    public Object invokeOnRowForProperty(Row row, String propertyName, Class<?> valueClass)
    {
        try
        {

            return getRowMethod(valueClass).invoke(row, propertyName);
        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public List<?> invokeOnRowForList(Row row, String propertyName, Class<?> valueClass)
    {
        try {
            return row.getList(propertyName, toCompatibleJavaType(valueClass));
        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve list property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Set<?> invokeOnRowForSet(Row row, String propertyName, Class<?> valueClass)
    {
        try {
            return row.getSet(propertyName, toCompatibleJavaType(valueClass));
        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve set property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Map<?, ?> invokeOnRowForMap(Row row, String propertyName, Class<?> keyClass,
            Class<?> valueClass)
    {
        try
        {
            return row.getMap(propertyName, toCompatibleJavaType(valueClass), toCompatibleJavaType(valueClass));
        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve map property '" + propertyName
                    + "' from CQL Row", e);
        }
    }
}
