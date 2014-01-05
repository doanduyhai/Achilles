package info.archinnov.achilles.internal.persistence.metadata;

import static info.archinnov.achilles.internal.helper.LoggerHelper.fqcnToStringFn;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public abstract class AbstractComponentProperties {

	protected final List<Class<?>> componentClasses;
	protected final List<String> componentNames;
	protected final List<Field> componentFields;
	protected final List<Method> componentGetters;
	protected final List<Method> componentSetters;

	protected AbstractComponentProperties(List<Class<?>> componentClasses, List<String> componentNames,
			List<Field> componentFields,List<Method> componentGetters, List<Method> componentSetters) {
		this.componentClasses = componentClasses;
		this.componentNames = componentNames;
        this.componentFields = componentFields;
        this.componentGetters = componentGetters;
		this.componentSetters = componentSetters;
	}

	public List<Class<?>> getComponentClasses() {
		return componentClasses;
	}

    public List<String> getComponentNames() {
		return componentNames;
	}

    public List<Method> getComponentGetters() {
		return componentGetters;
	}

    public List<Method> getComponentSetters() {
		return componentSetters;
	}

    public List<Field> getComponentFields() {
        return componentFields;
    }

    @Override
	public String toString() {

		return Objects.toStringHelper(this.getClass())
				.add("componentClasses", StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ","))
				.add("componentNames", componentNames).toString();

	}
}
