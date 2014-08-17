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

import java.util.ArrayList;
import java.util.List;

public class EmbeddedIdPropertiesBuilder {

	private final List<PropertyMeta> propertyMetas = new ArrayList<>();
	private List<ClusteringOrder> clusteringOrders;

	public void addPropertyMeta(PropertyMeta propertyMeta) {
		propertyMetas.add(propertyMeta);
	}

    public List<PropertyMeta> getPropertyMetas() {
        return propertyMetas;
    }

    public void setClusteringOrders(List<ClusteringOrder> clusteringOrders) {
        this.clusteringOrders = clusteringOrders;
	}

	public PartitionComponents buildPartitionKeys() {
		return new PartitionComponents(propertyMetas);
	}

	public ClusteringComponents buildClusteringKeys() {
		return new ClusteringComponents(propertyMetas, clusteringOrders);
	}

    public static EmbeddedIdProperties buildEmbeddedIdProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents, String entityName) {
        final List<PropertyMeta> propertyMetas = new ArrayList<>(partitionComponents.propertyMetas);
        propertyMetas.addAll(clusteringComponents.propertyMetas);
        return new EmbeddedIdProperties(partitionComponents, clusteringComponents, propertyMetas, entityName);
    }
}
