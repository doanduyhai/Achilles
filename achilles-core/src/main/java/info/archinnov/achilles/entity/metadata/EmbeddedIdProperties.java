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
package info.archinnov.achilles.entity.metadata;

import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.List;

import com.google.common.base.Objects;

public class EmbeddedIdProperties extends AbstractComponentProperties {

	private final PartitionKeys partitionKeys;
	private final ClusteringKeys clusteringKeys;
	private final List<String> timeUUIDComponents;

	public EmbeddedIdProperties(PartitionKeys partitionKeys, ClusteringKeys clusteringKeys,
			List<Class<?>> componentClasses, List<String> componentNames, List<Method> componentGetters,
			List<Method> componentSetters, List<String> timeUUIDComponents) {
		super(componentClasses, componentNames, componentGetters, componentSetters);
		this.partitionKeys = partitionKeys;
		this.clusteringKeys = clusteringKeys;
		this.timeUUIDComponents = timeUUIDComponents;
	}

	public boolean isCompositePartitionKey() {
		return partitionKeys.isComposite();
	}

	public boolean isClustered() {
		return clusteringKeys.isClustered();
	}

	public String getOrderingComponent() {
		return clusteringKeys.getOrderingComponent();
	}

	public List<String> getClusteringComponentNames() {
		return clusteringKeys.getComponentNames();
	}

	public List<Class<?>> getClusteringComponentClasses() {
		return clusteringKeys.getComponentClasses();
	}

	public List<String> getPartitionComponentNames() {
		return partitionKeys.getComponentNames();
	}

	public List<Class<?>> getPartitionComponentClasses() {
		return partitionKeys.getComponentClasses();
	}

	public List<Method> getPartitionComponentSetters() {
		return partitionKeys.getComponentSetters();
	}

	@Override
	public List<Class<?>> getComponentClasses() {
		return componentClasses;
	}

	@Override
	public List<Method> getComponentGetters() {
		return componentGetters;
	}

	@Override
	public List<Method> getComponentSetters() {
		return componentSetters;
	}

	@Override
	public List<String> getComponentNames() {
		return componentNames;
	}

	public List<String> getTimeUUIDComponents() {
		return timeUUIDComponents;
	}

	@Override
	public String toString() {

		return Objects.toStringHelper(this.getClass()).add("partitionKeys", partitionKeys)
				.add("clusteringKeys", clusteringKeys).toString();

	}

	public List<Object> extractPartitionComponents(List<Object> components) {
		int partitionComponentsCount = partitionKeys.getComponentClasses().size();

		Validator.validateTrue(components.size() >= partitionComponentsCount,
				"Cannot extract composite partition key components from components list '%s'", components);
		return components.subList(0, partitionComponentsCount);
	}

	public List<Object> extractClusteringComponents(List<Object> components) {
		int partitionComponentsCount = partitionKeys.getComponentClasses().size();

		Validator.validateTrue(components.size() >= partitionComponentsCount,
				"Cannot extract clustering components from components list '%s'", components);
		return components.subList(partitionComponentsCount, components.size());
	}
}
