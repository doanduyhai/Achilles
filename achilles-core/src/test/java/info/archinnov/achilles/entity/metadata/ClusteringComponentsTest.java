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

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.test.mapping.entity.UserBean;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClusteringComponentsTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ClusteringComponents clusteringComponents;

	@Test
	public void should_validate_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class), null,
				null, null);

		clusteringComponents.validateClusteringComponents("entityClass", Arrays.<Object> asList("name", 13));
	}

	@Test
	public void should_exception_when_no_clustering_component_provided() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class), null,
				null, null);
		exception.expect(AchillesException.class);
		exception
				.expectMessage("There should be at least one clustering key provided for querying on entity 'entityClass'");

		clusteringComponents.validateClusteringComponents("entityClass", null);
	}

	@Test
	public void should_exception_when_wrong_type_provided_for_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,
				UUID.class), null, null, null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("The type 'java.lang.Long' of clustering key '15' for querying on entity 'entityClass' is not valid. It should be 'java.lang.Integer'");

		clusteringComponents.validateClusteringComponents("entityClass",
				Arrays.<Object> asList("name", 15L, UUID.randomUUID()));
	}

	@Test
	public void should_exception_when_too_many_values_for_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,
				UUID.class), null, null, null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("There should be at most 3 value(s) of clustering component(s) provided for querying on entity 'entityClass'");

		clusteringComponents.validateClusteringComponents("entityClass",
				Arrays.<Object> asList("name", 15L, UUID.randomUUID(), 15));
	}

	@Test
	public void should_exception_when_null_value_between_clustering_components() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,
				UUID.class), null, null, null);

		exception.expect(AchillesException.class);
		exception.expectMessage("There should not be any null value between two non-null components of an @EmbeddedId");

		clusteringComponents.validateClusteringComponents("entityClass",
				Arrays.<Object> asList("name", null, UUID.randomUUID()));
	}

	@Test
	public void should_exception_when_component_not_comparable_for_clustering_component() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(String.class, Integer.class,
				UserBean.class), null, null, null);

		UserBean userBean = new UserBean();
		exception.expect(AchillesException.class);
		exception.expectMessage("The type '" + UserBean.class.getCanonicalName() + "' of clustering key '" + userBean
				+ "' for querying on entity 'entityClass' should implement the Comparable<T> interface");

		clusteringComponents.validateClusteringComponents("entityClass", Arrays.<Object> asList("name", 15, userBean));
	}

	@Test
	public void should_skip_validation_when_null_clustering_value() throws Exception {
		clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(Long.class, String.class,
				Integer.class, UserBean.class), null, null, null);

		clusteringComponents.validateClusteringComponents("entityClass", Arrays.<Object> asList(null, null, null));
	}

}
