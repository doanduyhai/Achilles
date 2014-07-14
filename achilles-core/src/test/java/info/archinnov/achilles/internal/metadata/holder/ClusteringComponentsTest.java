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

import java.util.Arrays;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.metadata.holder.ClusteringComponents;
import info.archinnov.achilles.test.mapping.entity.UserBean;

@RunWith(MockitoJUnitRunner.class)
public class ClusteringComponentsTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ClusteringComponents clusteringComponents;

	@Test
	public void should_validate_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class), null,null,null, null, null);

		clusteringComponents.validateClusteringComponents("entityClass", "name", 13);
	}

	@Test
	public void should_exception_when_no_clustering_component_provided() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class), null,null,null, null,null);
		exception.expect(AchillesException.class);
		exception.expectMessage("There should be at least one clustering key provided for querying on entity 'entityClass'");

		clusteringComponents.validateClusteringComponents("entityClass", null);
	}

	@Test
	public void should_exception_when_wrong_type_provided_for_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,UUID.class), null,
                null, null,null,null);

		exception.expect(AchillesException.class);
		exception.expectMessage("The type 'java.lang.Long' of clustering key '15' for querying on entity 'entityClass' is not valid. It should be 'java.lang.Integer'");

		clusteringComponents.validateClusteringComponents("entityClass","name", 15L, UUID.randomUUID());
	}

	@Test
	public void should_exception_when_too_many_values_for_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,UUID.class), null,null, null, null,null);

		exception.expect(AchillesException.class);
		exception.expectMessage("There should be at most 3 value(s) of clustering component(s) provided for querying on entity 'entityClass'");

		clusteringComponents.validateClusteringComponents("entityClass","name", 15L, UUID.randomUUID(), 15);
	}

	@Test
	public void should_exception_when_null_value() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,UUID.class),null, null, null, null,null);

		exception.expect(AchillesException.class);
		exception.expectMessage("The '2th' clustering key should not be null");

		clusteringComponents.validateClusteringComponents("entityClass","name", null, UUID.randomUUID());
	}
}
