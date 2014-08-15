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
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.validation.Validator;

public class ClusteringComponents extends AbstractComponentProperties {

    private List<ClusteringOrder> clusteringOrders;

    public ClusteringComponents(List<Class<?>> componentClasses, List<String> componentNames, List<Field> componentFields, List<Method> componentGetters, List<Method> componentSetters, List<ClusteringOrder> clusteringOrders) {
        super(componentClasses, componentNames, componentFields, componentGetters, componentSetters);
        this.clusteringOrders = clusteringOrders;
    }

    void validateClusteringComponents(String className, Object... clusteringComponentsArray) {

        Validator.validateTrue(ArrayUtils.isNotEmpty(clusteringComponentsArray), "There should be at least one clustering key provided for querying on entity '%s'", className);

        final List<Object> clusteringComponents = Arrays.asList(clusteringComponentsArray);

        int maxClusteringCount = componentClasses.size();

        Validator.validateTrue(clusteringComponents.size() <= maxClusteringCount,
                "There should be at most %s value(s) of clustering component(s) provided for querying on entity '%s'",
                maxClusteringCount, className);

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

        Validator.validateTrue(ArrayUtils.isNotEmpty(clusteringComponentsInArray), "There should be at least one clustering key IN provided for querying on entity '%s'", className);

        final List<Object> clusteringComponentsIn = Arrays.asList(clusteringComponentsInArray);

        Class<?> lastClusteringComponentType = componentClasses.get(componentClasses.size() - 1);

        for (int i = 0; i < clusteringComponentsIn.size(); i++) {
            Object clusteringKey = clusteringComponentsIn.get(i);
            Validator.validateNotNull(clusteringKey, "The '%sth' clustering key should not be null", i + 1);
            Class<?> currentClusteringType = clusteringKey.getClass();

            Validator.validateTrue(isCompatibleClass(lastClusteringComponentType,currentClusteringType), "The type '%s' of clustering key '%s' for querying on entity '%s' is not valid. It should be '%s'",
                    currentClusteringType.getCanonicalName(), clusteringKey, className, lastClusteringComponentType.getCanonicalName());
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

}
