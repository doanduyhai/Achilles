package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.helper.LoggerHelper.*;
import static info.archinnov.achilles.helper.LoggerHelper.fqcnToStringFn;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public abstract class AbstractComponentProperties {

	protected final List<Class<?>> componentClasses;
	protected final List<String> componentNames;
	protected final List<Method> componentGetters;
	protected final List<Method> componentSetters;

	protected AbstractComponentProperties(List<Class<?>> componentClasses, List<String> componentNames,
			List<Method> componentGetters, List<Method> componentSetters) {
		this.componentClasses = componentClasses;
		this.componentNames = componentNames;
		this.componentGetters = componentGetters;
		this.componentSetters = componentSetters;
	}

	List<Class<?>> getComponentClasses() {
		return componentClasses;
	}

	List<String> getComponentNames() {
		return componentNames;
	}

	List<Method> getComponentGetters() {
		return componentGetters;
	}

	List<Method> getComponentSetters() {
		return componentSetters;
	}

	@Override
	public String toString() {

		return Objects.toStringHelper(this.getClass())
				.add("componentClasses", StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ","))
				.add("componentNames", componentNames).toString();

	}
}
