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

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class EmbeddedIdPropertiesTest {
	@Test
	public void should_to_string() throws Exception {
		List<Class<?>> componentClasses = Arrays.<Class<?>> asList(
				Integer.class, String.class);
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setComponentClasses(componentClasses);
		props.setComponentNames(Arrays.asList("id", "age"));

		StringBuilder toString = new StringBuilder();
		toString.append("EmbeddedIdProperties{componentClasses=");
		toString.append("java.lang.Integer,java.lang.String, componentNames=[id, age]}");

		assertThat(props.toString()).isEqualTo(toString.toString());
	}

	@Test
	public void should_get_cql_ordering_component() throws Exception {
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setComponentNames(Arrays.asList("id", "age", "label"));

		assertThat(props.getOrderingComponent()).isEqualTo("age");
	}

	@Test
	public void should_return_null_if_no_cql_ordering_component()
			throws Exception {
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setComponentNames(Arrays.asList("id"));

		assertThat(props.getOrderingComponent()).isNull();
	}

	@Test
	public void should_get_clustering_component_names() throws Exception {
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setComponentNames(Arrays.asList("id", "comp1", "comp2"));

		assertThat(props.getClusteringComponentNames()).containsExactly(
				"comp1", "comp2");
	}

	@Test
	public void should_get_clustering_component_classes() throws Exception {
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setComponentClasses(Arrays.<Class<?>> asList(Long.class,
				UUID.class, String.class));

		assertThat(props.getClusteringComponentClasses()).containsExactly(
				UUID.class, String.class);
	}
}
