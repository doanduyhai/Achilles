package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.helper.LoggerHelper.*;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;

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
	private List<Serializer<?>> componentSerializers;
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

	public List<Serializer<?>> getComponentSerializers()
	{
		return componentSerializers;
	}

	public void setComponentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
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
		return "MultiKeyProperties [componentClasses="
				+ StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ",")
				+ ", componentSerializers="
				+ StringUtils.join(Lists.transform(componentSerializers, srzToStringFn), ",") + "]";
	}

}
