package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.cql.CQLTypeMapper.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import java.util.ArrayList;
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
    public Object invokeOnRowForFields(Row row, PropertyMeta pm)
    {
        String propertyName = pm.getPropertyName();
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

    public Object invokeOnRowForCompoundKey(Row row, PropertyMeta pm)
    {
        List<String> componentNames = pm.getComponentNames();
        List<Class<?>> componentClasses = pm.getComponentClasses();
        List<Object> rawValues = new ArrayList<Object>();

        try {
            for (int i = 0; i < componentNames.size(); i++)
            {
                String componentName = componentNames.get(i);
                Class<?> componentClass = componentClasses.get(i);
                if (row.isNull(componentName)) {
                    throw new AchillesException("Error, the component '" + componentName
                            + "' from @CompoundKey class '"
                            + pm.getValueClass() + "' cannot be found from Cassandra");
                }
                else {
                    Object rawValue = getRowMethod(componentClass).invoke(row, componentName);
                    rawValues.add(rawValue);
                }
            }
            return pm.decodeFromComponents(rawValues);
        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve compound property '" + pm.getPropertyName()
                    + "' for entity class '" + pm.getEntityClassName() + "' from CQL Row", e);
        }

    }

    public Object invokeOnRowForProperty(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass)
    {
        try
        {
            Object rawValue = getRowMethod(valueClass).invoke(row, propertyName);
            return pm.decode(rawValue);
        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve property '" + propertyName
                    + "' for entity class '" + pm.getEntityClassName() + "' from CQL Row", e);
        }
    }

    public List<?> invokeOnRowForList(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass)
    {
        try {
            List<?> rawValues = row.getList(propertyName, toCompatibleJavaType(valueClass));
            return pm.decode(rawValues);

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve list property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Set<?> invokeOnRowForSet(Row row, PropertyMeta pm, String propertyName, Class<?> valueClass)
    {
        try {
            Set<?> rawValues = row.getSet(propertyName, toCompatibleJavaType(valueClass));
            return pm.decode(rawValues);

        } catch (Exception e) {
            throw new AchillesException("Cannot retrieve set property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Map<?, ?> invokeOnRowForMap(Row row, PropertyMeta pm, String propertyName, Class<?> keyClass,
            Class<?> valueClass)
    {
        try
        {
            Map<?, ?> rawValues = row.getMap(propertyName, toCompatibleJavaType(keyClass),
                    toCompatibleJavaType(valueClass));
            return pm.decode(rawValues);

        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve map property '" + propertyName
                    + "' from CQL Row", e);
        }
    }

    public Object invokeOnRowForType(Row row, Class<?> type, String name)
    {
        try
        {
            return getRowMethod(type).invoke(row, name);
        } catch (Exception e)
        {
            throw new AchillesException("Cannot retrieve column '" + name
                    + "' of type '" + type.getCanonicalName() + "' from CQL Row", e);
        }
    }
}
