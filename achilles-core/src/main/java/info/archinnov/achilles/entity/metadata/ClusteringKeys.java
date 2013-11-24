package info.archinnov.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.List;

public class ClusteringKeys extends AbstractComponentProperties {

	public ClusteringKeys(List<Class<?>> componentClasses,
			List<String> componentNames, List<Method> componentGetters,
			List<Method> componentSetters) {
		super(componentClasses, componentNames, componentGetters,
				componentSetters);
	}

	String getOrderingComponent() {
		return isClustered() ? componentNames.get(0) : null;
	}

	boolean isClustered() {
		return componentClasses.size() > 0;
	}
}
