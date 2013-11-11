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

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionComponentsTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private PartitionComponents partitionComponents;

	@Test
	public void should_validate_partition_components() throws Exception {
		partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class), null, null,
				null);

		partitionComponents.validatePartitionComponents("classname", Arrays.<Object> asList(11L, "type"));
	}

	@Test
	public void should_exception_when_no_partition_component_provided() throws Exception {
		partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class), null, null,
				null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("There should be at least one partition key component provided for querying on entity 'entityClass'");

		partitionComponents.validatePartitionComponents("entityClass", null);
	}

	@Test
	public void should_exception_when_empty_list_of_partition_component_provided() throws Exception {
		partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class), null, null,
				null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("There should be at least one partition key component provided for querying on entity 'entityClass'");

		partitionComponents.validatePartitionComponents("entityClass", Arrays.<Object> asList());
	}

	@Test
	public void should_exception_when_null_partition_component_provided() throws Exception {
		partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class), null, null,
				null);

		exception.expect(AchillesException.class);
		exception.expectMessage("The '2th' partition component should not be null");

		partitionComponents.validatePartitionComponents("entityClass", Arrays.<Object> asList(10L, null));
	}

	@Test
	public void should_exception_when_partition_component_count_doest_not_match() throws Exception {
		partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, String.class), null, null,
				null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("There should be exactly '2' partition components for querying on entity 'entityClass'");

		partitionComponents.validatePartitionComponents("entityClass", Arrays.<Object> asList(11L, "test", 11));
	}

	@Test
	public void should_exception_when_incorrect_type_of_partition_component_provided() throws Exception {
		partitionComponents = new PartitionComponents(Arrays.<Class<?>> asList(Long.class, Long.class), null, null,
				null);

		exception.expect(AchillesException.class);
		exception
				.expectMessage("The type 'java.lang.String' of partition key component 'name' for querying on entity 'entityClass' is not valid. It should be 'java.lang.Long'");

		partitionComponents.validatePartitionComponents("entityClass", Arrays.<Object> asList(11L, "name"));
	}
}
