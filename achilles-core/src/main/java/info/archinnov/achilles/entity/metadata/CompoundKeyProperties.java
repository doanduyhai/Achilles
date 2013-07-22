package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.helper.LoggerHelper.fqcnToStringFn;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * CompoundKeyProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompoundKeyProperties
{
    private List<Class<?>> componentClasses;
    private List<String> componentNames;
    private List<Method> componentGetters;
    private List<Method> componentSetters;
    private Constructor<?> constructor;

    private Function<String, String> lowerCase = new Function<String, String>()
    {
        @Override
        public String apply(String name)
        {
            return name.toLowerCase();
        }
    };

    public String getCQLOrderingComponent()
    {
        String component = null;
        if (componentNames.size() > 1)
        {
            return componentNames.get(1).toLowerCase();
        }
        return component;
    }

    public List<Class<?>> getComponentClasses()
    {
        return componentClasses;
    }

    public void setComponentClasses(List<Class<?>> componentClasses)
    {
        this.componentClasses = componentClasses;
    }

    public List<Method> getComponentGetters()
    {
        return componentGetters;
    }

    public void setComponentGetters(List<Method> componentGetters)
    {
        this.componentGetters = componentGetters;
    }

    public List<Method> getComponentSetters()
    {
        return componentSetters;
    }

    public void setComponentSetters(List<Method> componentSetters)
    {
        this.componentSetters = componentSetters;
    }

    public List<String> getComponentNames()
    {
        return componentNames;
    }

    public void setComponentNames(List<String> componentNames)
    {
        this.componentNames = componentNames;
    }

    public <T> Constructor<T> getConstructor()
    {
        return (Constructor<T>) constructor;
    }

    public void setConstructor(Constructor<?> constructor)
    {
        this.constructor = constructor;
    }

    @Override
    public String toString()
    {
        return "CompoundKeyProperties [componentClasses=["
                + StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ",")
                + "], componentNames=" + componentNames + "]";
    }

}
