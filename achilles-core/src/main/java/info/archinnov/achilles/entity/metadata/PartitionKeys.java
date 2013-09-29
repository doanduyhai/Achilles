package info.archinnov.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.List;

public class PartitionKeys extends AbstractComponentProperties {

	public PartitionKeys(List<Class<?>> componentClasses, List<String> componentNames, List<Method> componentGetters,
			List<Method> componentSetters) {
		super(componentClasses, componentNames, componentGetters, componentSetters);
	}

	boolean isComposite() {
		return this.componentClasses.size() > 1;
	}

	Class<?> getPartitionKeyClass() {
		return componentClasses.get(0);
	}

	String getPartitionKeyName() {
		return componentNames.get(0);
	}

	Method getPartitionKeyGetter() {
		return componentGetters.get(0);
	}

	Method getPartitionKeySetter() {
		return componentSetters.get(0);
	}
}
