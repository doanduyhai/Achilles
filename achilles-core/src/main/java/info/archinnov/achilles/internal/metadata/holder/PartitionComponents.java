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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.validation.Validator;

public class PartitionComponents extends AbstractComponentProperties {

    private static final Logger log  = LoggerFactory.getLogger(PartitionComponents.class);

    public PartitionComponents(List<Class<?>> componentClasses, List<String> componentNames,
        List<Field> componentFields,List<Method> componentGetters, List<Method> componentSetters) {
		super(componentClasses, componentNames, componentFields, componentGetters, componentSetters);
	}

	void validatePartitionComponents(String className, List<Object> partitionComponents) {
        log.trace("Validate partition components {} of entity class {}",partitionComponents,className);

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
