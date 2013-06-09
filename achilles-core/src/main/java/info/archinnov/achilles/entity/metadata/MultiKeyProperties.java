package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.helper.LoggerHelper.fqcnToStringFn;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * MultiKeyProperties
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyProperties
{
	private List<Class<?>> componentClasses;
	private List<String> componentNames;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	private Function<String, String> lowerCase = new Function<String, String>()
	{
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

	public List<String> getCQLComponentNames()
	{
		return FluentIterable.from(componentNames).transform(lowerCase).toImmutableList();
	}

	public void setComponentNames(List<String> componentNames)
	{
		this.componentNames = componentNames;
	}

	@Override
	public String toString()
	{
		return "MultiKeyProperties [componentClasses=["
				+ StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ",")
				+ "], componentNames=" + componentNames + "]";
	}

}
