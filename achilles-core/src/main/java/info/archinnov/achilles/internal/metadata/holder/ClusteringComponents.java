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
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusteringComponents extends AbstractComponentProperties {

    private static final Logger log = LoggerFactory.getLogger(ClusteringComponents.class);

    private List<ClusteringOrder> clusteringOrders;

    public ClusteringComponents(List<PropertyMeta> clusteringKeyMetas, List<ClusteringOrder> clusteringOrders) {
        super(clusteringKeyMetas);
        this.clusteringOrders = clusteringOrders;
    }

    void validateClusteringComponents(String className, Object... clusteringComponentsArray) {
        Validator.validateTrue(isNotEmpty(clusteringComponentsArray), "There should be at least one clustering key provided for querying on entity '%s'", className);

        final List<Object> clusteringComponents = asList(clusteringComponentsArray);

        log.trace("Validate clustering components {} for slice query on entity class {}", clusteringComponents, className);

        final List<Class<?>> componentClasses = getComponentClasses();

        int maxClusteringCount = componentClasses.size();

        Validator.validateTrue(clusteringComponents.size() <= maxClusteringCount,
                "There should be at most %s value(s) of clustering component(s) provided for querying on entity '%s'",maxClusteringCount, className);

        for (int i = 0; i < clusteringComponents.size(); i++) {
            Object clusteringKey = clusteringComponents.get(i);
            Validator.validateNotNull(clusteringKey, "The '%sth' clustering key should not be null", i + 1);
            Class<?> currentClusteringType = clusteringKey.getClass();
            Class<?> expectedClusteringType = componentClasses.get(i);

            Validator.validateTrue(isCompatibleClass(expectedClusteringType,currentClusteringType), "The type '%s' of clustering key '%s' for querying on entity '%s' is not valid. It should be '%s'",
                    currentClusteringType.getCanonicalName(), clusteringKey, className, expectedClusteringType.getCanonicalName());
        }
    }

    void validateClusteringComponentsIn(String className, Object... clusteringComponentsInArray) {
        final List<Class<?>> componentClasses = getComponentClasses();

        Validator.validateTrue(isNotEmpty(clusteringComponentsInArray), "There should be at least one clustering key IN provided for querying on entity '%s'", className);

        final List<Object> clusteringComponentsIn = asList(clusteringComponentsInArray);

        log.trace("Validate clustering components IN {} for slice query on entity class {}", clusteringComponentsIn, className);

        Class<?> lastClusteringComponentType = componentClasses.get(componentClasses.size() - 1);

        for (int i = 0; i < clusteringComponentsIn.size(); i++) {
            Object clusteringKey = clusteringComponentsIn.get(i);
            Validator.validateNotNull(clusteringKey, "The '%sth' clustering key should not be null", i + 1);
            Class<?> currentClusteringType = clusteringKey.getClass();

            Validator.validateTrue(isCompatibleClass(lastClusteringComponentType,currentClusteringType), "The type '%s' of clustering key '%s' for querying on entity '%s' is not valid. It should be '%s'",
                    currentClusteringType.getCanonicalName(), clusteringKey, className, lastClusteringComponentType.getCanonicalName());
        }
    }

    boolean isClustered() {
        return getComponentClasses().size() > 0;
    }

    public List<ClusteringOrder> getClusteringOrders() {
        return clusteringOrders;
    }

}
