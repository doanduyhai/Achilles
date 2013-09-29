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

import static org.fest.assertions.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class EmbeddedIdPropertiesTest {

	private static final List<Class<?>> noClasses = Arrays.asList();
	private static final List<String> noNames = Arrays.asList();
	private static final List<Method> noAccessors = Arrays.asList();
	private static final List<String> noTimeUUID = Arrays.asList();

	@Test
	public void should_to_string() throws Exception {

		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(Arrays.<Class<?>> asList(UUID.class), Arrays.asList("date"),
				noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		StringBuilder toString = new StringBuilder();
		toString.append("EmbeddedIdProperties{");
		toString.append("partitionKeys=PartitionKeys{componentClasses=java.lang.Long, componentNames=[id]}, ");
		toString.append("clusteringKeys=ClusteringKeys{componentClasses=java.util.UUID, componentNames=[date]}}");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}

	@Test
	public void should_get_ordering_component() throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getOrderingComponent()).isEqualTo("date");
	}

	@Test
	public void should_return_null_if_no_ordering_component() throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(noClasses, noNames, noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getOrderingComponent()).isNull();
	}

	@Test
	public void should_get_clustering_component_names_and_classes() throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getClusteringComponentNames()).containsExactly("date", "name");
		assertThat(props.getClusteringComponentClasses()).containsExactly(UUID.class, String.class);
		assertThat(props.isClustered()).isTrue();
	}

	@Test
	public void should_get_partition_component_names_and_classes() throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("id", "type"), noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		assertThat(props.getPartitionComponentNames()).containsExactly("id", "type");
		assertThat(props.getPartitionComponentClasses()).containsExactly(Long.class, String.class);

		assertThat(props.isCompositePartitionKey()).isTrue();
	}

	@Test
	public void should_extract_partition_and_clustering_components_for_compound_partition_clustered_entity()
			throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("id", "type"), noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		UUID date = new UUID(10, 10);
		List<Object> components = Arrays.<Object> asList(10L, "type", date, "name");

		assertThat(props.extractPartitionComponents(components)).containsExactly(10L, "type");
		assertThat(props.extractClusteringComponents(components)).containsExactly(date, "name");
	}

	@Test
	public void should_extract_partition_and_clustering_components_for_non_cluster_compound_partition_key()
			throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("id", "type"), noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(noClasses, noNames, noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		List<Object> components = Arrays.<Object> asList(10L, "type");

		assertThat(props.extractPartitionComponents(components)).containsExactly(10L, "type");
		assertThat(props.extractClusteringComponents(components)).isEmpty();
	}

	@Test
	public void should_extract_partition_and_clustering_components_for_simple_clustered_entity() throws Exception {
		PartitionKeys partitionKeys = new PartitionKeys(Arrays.<Class<?>> asList(Long.class), Arrays.asList("id"),
				noAccessors, noAccessors);

		ClusteringKeys clusteringKeys = new ClusteringKeys(Arrays.<Class<?>> asList(UUID.class, String.class),
				Arrays.asList("date", "name"), noAccessors, noAccessors);

		EmbeddedIdProperties props = new EmbeddedIdProperties(partitionKeys, clusteringKeys, noClasses, noNames,
				noAccessors, noAccessors, noTimeUUID);

		UUID date = new UUID(10, 10);
		List<Object> components = Arrays.<Object> asList(10L, date, "name");

		assertThat(props.extractPartitionComponents(components)).containsExactly(10L);
		assertThat(props.extractClusteringComponents(components)).containsExactly(date, "name");
	}
}
