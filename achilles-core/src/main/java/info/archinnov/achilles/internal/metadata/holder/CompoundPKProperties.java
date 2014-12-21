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

import com.google.common.base.Objects;

public class CompoundPKProperties extends AbstractComponentProperties {

    private final PartitionComponents partitionComponents;
    private final ClusteringComponents clusteringComponents;
    final String entityClassName;

    public CompoundPKProperties(PartitionComponents partitionComponents, ClusteringComponents clusteringComponents, List<PropertyMeta> keyMetas, String entityClassName) {
		super(keyMetas);
		this.partitionComponents = partitionComponents;
		this.clusteringComponents = clusteringComponents;
        this.entityClassName = entityClassName;
    }


	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("partitionComponents", partitionComponents).add("clusteringComponents", clusteringComponents).toString();
	}

    public PartitionComponents getPartitionComponents() {
        return partitionComponents;
    }

    public ClusteringComponents getClusteringComponents() {
        return clusteringComponents;
    }
}
