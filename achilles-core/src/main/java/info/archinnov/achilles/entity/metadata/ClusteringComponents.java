package info.archinnov.achilles.entity.metadata;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import info.archinnov.achilles.validation.Validator;

public class ClusteringComponents extends AbstractComponentProperties {

	private final String reversedComponentName;

	public ClusteringComponents(List<Class<?>> componentClasses, List<String> componentNames,
                                String reversedComponentName,
                                List<Method> componentGetters, List<Method> componentSetters) {
		super(componentClasses, componentNames, componentGetters, componentSetters);
		this.reversedComponentName = reversedComponentName;
	}

	public ClusteringComponents(List<Class<?>> componentClasses, List<String> componentNames, List<Method> componentGetters,
                                List<Method> componentSetters) {
		this(componentClasses, componentNames, null, componentGetters, componentSetters);
	}

    void validateClusteringComponents(String className, List<Object> clusteringKeys) {
        Validator.validateNotNull(clusteringKeys,
                                  "There should be at least one clustering key provided for querying on entity '%s'", className);

        int maxClusteringCount = componentClasses.size();

        Validator.validateTrue(clusteringKeys.size() <= maxClusteringCount,
                               "There should be at most %s value(s) of clustering component(s) provided for querying on entity '%s'",
                               maxClusteringCount, className);

        validateNoHoleAndReturnLastNonNullIndex(Arrays.<Object> asList(clusteringKeys));

        for (int i = 0; i < clusteringKeys.size(); i++) {
            Object clusteringKey = clusteringKeys.get(i);
            if (clusteringKey != null) {
                Class<?> clusteringType = clusteringKey.getClass();
                Class<?> expectedClusteringType = componentClasses.get(i);

                Validator
                        .validateComparable(
                                clusteringType,
                                "The type '%s' of clustering key '%s' for querying on entity '%s' should implement the Comparable<T> interface",
                                clusteringType.getCanonicalName(), clusteringKey, className);

                Validator
                        .validateTrue(
                                expectedClusteringType.equals(clusteringType),
                                "The type '%s' of clustering key '%s' for querying on entity '%s' is not valid. It should be '%s'",
                                clusteringType.getCanonicalName(), clusteringKey, className,
                                expectedClusteringType.getCanonicalName());
            }

        }
    }

	String getOrderingComponent() {
		return isClustered() ? componentNames.get(0) : null;
	}

	boolean isClustered() {
		return componentClasses.size() > 0;
	}

	public String getReversedComponent() {
		return reversedComponentName;
	}

    public boolean hasReversedComponent() {
        return StringUtils.isNotBlank(reversedComponentName);
    }

    private int validateNoHoleAndReturnLastNonNullIndex(List<Object> components) {
        boolean nullFlag = false;
        int lastNotNullIndex = 0;
        for (Object keyValue : components) {
            if (keyValue != null) {
                if (nullFlag) {
                    throw new IllegalArgumentException(
                            "There should not be any null value between two non-null components of a @EmbeddedId");
                }
                lastNotNullIndex++;
            } else {
                nullFlag = true;
            }
        }
        lastNotNullIndex--;

        return lastNotNullIndex;
    }
}
