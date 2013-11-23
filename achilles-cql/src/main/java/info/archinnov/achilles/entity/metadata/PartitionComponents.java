package info.archinnov.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.List;
import info.archinnov.achilles.validation.Validator;

public class PartitionComponents extends AbstractComponentProperties {

	public PartitionComponents(List<Class<?>> componentClasses, List<String> componentNames,
			List<Method> componentGetters, List<Method> componentSetters) {
		super(componentClasses, componentNames, componentGetters, componentSetters);
	}

	void validatePartitionComponents(String className, List<Object> partitionComponents) {
		Validator.validateNotNull(partitionComponents,
				"There should be at least one partition key component provided for querying on " + "entity '%s'",
				className);
		Validator.validateTrue(partitionComponents.size() > 0,
				"There should be at least one partition key component provided for querying on entity '%s'", className);

		Validator.validateTrue(partitionComponents.size() == componentClasses.size(),
				"There should be exactly '%s' partition components for querying on entity '%s'",
				componentClasses.size(), className);

		for (int i = 0; i < partitionComponents.size(); i++) {
			Object partitionKeyComponent = partitionComponents.get(i);
			Validator
					.validateNotNull(partitionKeyComponent, "The '%sth' partition component should not be null", i + 1);

			Class<?> currentPartitionComponentType = partitionKeyComponent.getClass();
			Class<?> expectedPartitionComponentType = componentClasses.get(i);

			Validator
					.validateTrue(
							currentPartitionComponentType.equals(expectedPartitionComponentType),
							"The type '%s' of partition key component '%s' for querying on entity '%s' is not valid. It should be '%s'",
							currentPartitionComponentType.getCanonicalName(), partitionKeyComponent, className,
							expectedPartitionComponentType.getCanonicalName());
		}
	}

	boolean isComposite() {
		return this.componentClasses.size() > 1;
	}
}
