/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.archinnov.achilles.compound;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.OrderingMode;
import info.archinnov.achilles.validation.Validator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public abstract class CompoundKeyValidator {
	protected ComponentComparator comparator = new ComponentComparator();

	public void validatePartitionKey(PropertyMeta idMeta, List<Object> partitionComponents) {
		String className = idMeta.getEntityClassName();
		Validator.validateNotNull(partitionComponents,
				"There should be at least one partition key component provided for querying on entity '%s'", className);
		Validator.validateTrue(partitionComponents.size() > 0,
				"There should be at least one partition key component provided for querying on entity '%s'", className);

		List<Class<?>> partitionComponentClasses = idMeta.getPartitionComponentClasses();

		Validator.validateTrue(partitionComponents.size() == partitionComponentClasses.size(),
				"There should be exactly '%s' partition components for for querying on entity '%s'",
				partitionComponentClasses.size(), className);

		for (int i = 0; i < partitionComponents.size(); i++) {
			Object partitionKeyComponent = partitionComponents.get(i);
			Class<?> currentPartitionComponentType = partitionKeyComponent.getClass();
			Class<?> expectedPartitionComponentType = partitionComponentClasses.get(i);

			Validator.validateNotNull(partitionKeyComponent, "The '%s' partition component should not be null", i);

			Validator
					.validateTrue(
							currentPartitionComponentType.equals(expectedPartitionComponentType),
							"The type '%s' of partition key component '%s' for querying on entity '%s' is not valid. It should be '%s'",
							currentPartitionComponentType.getCanonicalName(), partitionKeyComponent, className,
							expectedPartitionComponentType.getCanonicalName());
		}
	}

	public void validateClusteringKeys(PropertyMeta idMeta, List<Object> clusteringKeys) {
		String className = idMeta.getEntityClassName();
		Validator.validateNotNull(clusteringKeys,
				"There should be at least one clustering key provided for querying on entity '%s'", className);

		List<Class<?>> clusteringClasses = idMeta.getClusteringComponentClasses();
		int maxClusteringCount = clusteringClasses.size();

		Validator.validateTrue(clusteringKeys.size() <= maxClusteringCount,
				"There should be at most %s value(s) of clustering component(s) provided for querying on entity '%s'",
				maxClusteringCount, className);

		validateNoHoleAndReturnLastNonNullIndex(Arrays.<Object> asList(clusteringKeys));

		for (int i = 0; i < clusteringKeys.size(); i++) {
			Object clusteringKey = clusteringKeys.get(i);
			if (clusteringKey != null) {
				Class<?> clusteringType = clusteringKey.getClass();
				Class<?> expectedClusteringType = clusteringClasses.get(i);

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

	public void validateComponentsForSliceQuery(PropertyMeta propertyMeta, List<Object> start, List<Object> end,
			OrderingMode ordering) {
		Validator
				.validateNotNull(start.get(0), "Partition key should not be null for start clustering key : %s", start);
		Validator.validateNotNull(end.get(0), "Partition key should not be null for end clustering key : %s", end);
		Validator.validateTrue(start.get(0).equals(end.get(0)),
				"Partition key should be equal for start and end clustering keys : [%s,%s]", start, end);

		validateComponentsForSliceQuery(start.subList(0, start.size()), end.subList(0, end.size()), ordering);
	}

	public int validateNoHoleAndReturnLastNonNullIndex(List<Object> components) {
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

	public int getLastNonNullIndex(List<Object> components) {
		for (int i = 0; i < components.size(); i++) {
			if (components.get(i) == null) {
				return i - 1;
			}
		}
		return components.size() - 1;
	}

	public abstract void validateComponentsForSliceQuery(List<Object> startComponents, List<Object> endComponents,
			OrderingMode ordering);

	protected static class ComponentComparator implements Comparator<Object> {

		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object o1, Object o2) {
			if (o1.getClass().isEnum() && o2.getClass().isEnum()) {
				String name1 = ((Enum<?>) o1).name();
				String name2 = ((Enum<?>) o2).name();

				return name1.compareTo(name2);
			} else if (Comparable.class.isAssignableFrom(o1.getClass())
					&& Comparable.class.isAssignableFrom(o2.getClass())) {
				Comparable<Object> comp1 = (Comparable<Object>) o1;
				Comparable<Object> comp2 = (Comparable<Object>) o2;

				return comp1.compareTo(comp2);
			} else {
				throw new IllegalArgumentException("Type '" + o1.getClass().getCanonicalName() + "' or type '"
						+ o2.getClass().getCanonicalName() + "' should implements Comparable");
			}
		}
	}
}
