package info.archinnov.achilles.compound;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.Row;

public class CQLCompoundKeyMapper
{

    private CQLRowMethodInvoker cqlRowInvoker = new CQLRowMethodInvoker();
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public Object createFromRow(Row row, PropertyMeta<?, ?> pm)
    {
        boolean bySetter = pm.hasDefaultConstructorForCompoundKey();

        Constructor<Object> constructor = pm.getCompoundKeyConstructor();
        List<String> componentNames = pm.getCQLComponentNames();
        List<Class<?>> componentClasses = pm.getComponentClasses();
        List<Object> values = new ArrayList<Object>();
        List<Method> componentSetters = pm.getComponentSetters();

        Object compoundKey = null;

        for (int i = 0; i < componentNames.size(); i++)
        {
            String componentName = componentNames.get(i);
            Class<?> clazz = componentClasses.get(i);
            if (row.isNull(componentName))
            {
                throw new AchillesException("Error, the component '" + componentName
                        + "' from @CompoundKey class '"
                        + pm.getValueClass() + "' cannot be found from Cassandra");
            }
            else
            {
                values.add(cqlRowInvoker.invokeOnRowForClusteredComponent(row, pm, componentName, clazz));
            }
        }

        if (bySetter) {
            compoundKey = invoker.instanciate(constructor);
            for (int i = 0; i < values.size(); i++)
            {
                invoker.setValueToField(compoundKey, componentSetters.get(i), values.get(i));
            }
        }
        else {
            compoundKey = invoker.instanciate(constructor,
                    values.toArray(new Object[values.size()]));
        }

        return compoundKey;
    }

    public List<Object> extractComponents(Object primaryKey, PropertyMeta<?, ?> idMeta)
    {
        List<Object> values = new ArrayList<Object>();
        List<Method> componentGetters = idMeta.getComponentGetters();
        List<Class<?>> componentClasses = idMeta.getComponentClasses();

        for (int i = 0; i < componentGetters.size(); i++)
        {
            Method componentGetter = componentGetters.get(i);
            Class<?> clazz = componentClasses.get(i);

            Object valueFromField = invoker.getValueFromField(primaryKey, componentGetter);
            values.add(idMeta.writeValueToCassandra(clazz, valueFromField));
        }

        return values;
    }

}
