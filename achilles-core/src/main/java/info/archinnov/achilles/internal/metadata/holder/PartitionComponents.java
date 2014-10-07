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

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internal.validation.Validator;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;

public class PartitionComponents extends AbstractComponentProperties {

    private static final Logger log  = LoggerFactory.getLogger(PartitionComponents.class);

    public PartitionComponents(List<PropertyMeta> partitionKeyMetas) {
		super(partitionKeyMetas);
	}

	void validatePartitionComponents(String className, Object...partitionComponentsArray) {
        final List<Class<?>> componentClasses = getComponentClasses();

        Validator.validateTrue(isNotEmpty(partitionComponentsArray), "There should be at least one partition key component provided for querying on " + "entity '%s'", className);
        final List<Object> partitionComponents = asList(partitionComponentsArray);

        log.trace("Validate partition components {} for slice query on entity class {}",partitionComponents,className);

        Validator.validateTrue(partitionComponents.size() > 0,"There should be at least one partition key component provided for querying on entity '%s'", className);
        Validator.validateTrue(partitionComponents.size() <= componentClasses.size(),"The partition key components count should be less or equal to '%s' for querying on entity '%s'",componentClasses.size(), className);

		for (int i = 0; i < partitionComponents.size(); i++) {
			Object partitionKeyComponent = partitionComponents.get(i);
			Validator.validateNotNull(partitionKeyComponent, "The '%sth' partition key component should not be null", i + 1);

			Class<?> currentPartitionComponentType = partitionKeyComponent.getClass();
			Class<?> expectedPartitionComponentType = componentClasses.get(i);

			Validator.validateTrue(isCompatibleClass(expectedPartitionComponentType, currentPartitionComponentType),
							"The type '%s' of partition key component '%s' for querying on entity '%s' is not valid. It should be '%s'",
							currentPartitionComponentType.getCanonicalName(), partitionKeyComponent, className,
							expectedPartitionComponentType.getCanonicalName());
		}
	}

    void validatePartitionComponentsIn(String className,Object...partitionComponentsInArray) {
        final List<Class<?>> componentClasses = getComponentClasses();

        Validator.validateTrue(isNotEmpty(partitionComponentsInArray), "There should be at least one partition key component IN provided for querying on entity '%s'", className);
        final List<Object> partitionComponentsIn = asList(partitionComponentsInArray);

        log.trace("Validate partition components IN {} for slice query on entity class {}",partitionComponentsIn,className);

        Class<?> lastPartitionComponentType = componentClasses.get(componentClasses.size() - 1);

        for (int i = 0; i < partitionComponentsIn.size(); i++) {
            Object partitionKeyComponent = partitionComponentsIn.get(i);
            Validator.validateNotNull(partitionKeyComponent, "The '%sth' partition key component IN should not be null", i + 1);

            Class<?> currentPartitionComponentType = partitionKeyComponent.getClass();

            Validator.validateTrue(isCompatibleClass(lastPartitionComponentType, currentPartitionComponentType),
                    "The type '%s' of partition key component '%s' for querying on entity '%s' is not valid. It should be '%s'",
                    currentPartitionComponentType.getCanonicalName(), partitionKeyComponent, className,
                    lastPartitionComponentType.getCanonicalName());
        }
    }
}
