package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.helper.AchillesLoggerHelper.fqcnToStringFn;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.StringUtils;

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
	private List<Method> componentGetters;
	private List<Method> componentSetters;

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

	@Override
	public String toString()
	{
		return "MultiKeyProperties [componentClasses=["
				+ StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ",") + "]]";
	}

}
