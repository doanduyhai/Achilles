/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;

public class ClusteringComponents extends AbstractComponentProperties {

    private List<ClusteringOrder> clusteringOrders;

    public ClusteringComponents(List<Class<?>> componentClasses, List<String> componentNames,List<Field> componentFields,List<Method> componentGetters, List<Method> componentSetters,List<ClusteringOrder> clusteringOrders) {
		super(componentClasses, componentNames, componentFields, componentGetters, componentSetters);
        this.clusteringOrders = clusteringOrders;
    }

	void validateClusteringComponents(String className, List<Object> clusteringComponents) {
		Validator.validateNotNull(clusteringComponents,
				"There should be at least one clustering key provided for querying on entity '%s'", className);

		int maxClusteringCount = componentClasses.size();

		Validator.validateTrue(clusteringComponents.size() <= maxClusteringCount,
				"There should be at most %s value(s) of clustering component(s) provided for querying on entity '%s'",
				maxClusteringCount, className);

		validateNoHoleAndReturnLastNonNullIndex(clusteringComponents);

		for (int i = 0; i < clusteringComponents.size(); i++) {
			Object clusteringKey = clusteringComponents.get(i);
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

    public List<ClusteringOrder> getClusteringOrders() {
        return clusteringOrders;
    }

    private int validateNoHoleAndReturnLastNonNullIndex(List<Object> components) {
		boolean nullFlag = false;
		int lastNotNullIndex = 0;
		for (Object component : components) {
			if (component != null) {
				if (nullFlag) {
					throw new AchillesException(
							"There should not be any null value between two non-null components of an @EmbeddedId");
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
