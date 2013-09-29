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
package info.archinnov.achilles.table;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EmbeddedIdProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftComparatorTypeAliasFactoryTest {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private ThriftComparatorTypeAliasFactory factory;

	@Mock
	private EmbeddedIdProperties embeddedIdProperties;

	@Mock
	private PropertyMeta embeddedIdMeta;

	@Mock
	private PropertyMeta idMeta;

	@Mock
	private List<Method> componentGetters;

	@Before
	public void setUp() {
		when(idMeta.isEmbeddedId()).thenReturn(false);
		when(embeddedIdMeta.isEmbeddedId()).thenReturn(true);
	}

	@Test
	public void should_determine_composite_type_alias_for_clustering_components_creation() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).clusteringNames("name", "date")
				.clusteringClasses(String.class, UUID.class).build();

		String actual = factory.determineCompatatorTypeAliasForClusteringComponents(idMeta, true);

		assertThat(actual).isEqualTo("(UTF8Type,UUIDType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_clustering_components_creation_with_time_uuid()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).clusteringNames("name", "date")
				.clusteringClasses(String.class, UUID.class).compTimeUUID("date").build();

		String actual = factory.determineCompatatorTypeAliasForClusteringComponents(idMeta, true);

		assertThat(actual).isEqualTo("(UTF8Type,TimeUUIDType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_clustering_components_validation() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).clusteringNames("name", "date")
				.clusteringClasses(String.class, UUID.class).build();

		String actual = factory.determineCompatatorTypeAliasForClusteringComponents(idMeta, false);

		assertThat(actual).isEqualTo(
				"CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_clustering_components_validation_with_time_uuid()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).clusteringNames("name", "date")
				.clusteringClasses(String.class, UUID.class).type(EMBEDDED_ID).compTimeUUID("date").build();

		String actual = factory.determineCompatatorTypeAliasForClusteringComponents(idMeta, false);

		assertThat(actual).isEqualTo(
				"CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.TimeUUIDType)");
	}

	@Test
	public void should_determine_composite_type_alias_for_single_partition_key_creation() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(ID).build();

		Pair<String, String> actual = factory.determineKeyValidationAndAlias(idMeta, true);

		assertThat(actual.left).isEqualTo("LongType");
		assertThat(actual.right).isNull();
	}

	@Test
	public void should_determine_composite_type_alias_for_embedded_single_partition_key_creation() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, UUID.class).build();

		Pair<String, String> actual = factory.determineKeyValidationAndAlias(idMeta, true);

		assertThat(actual.left).isEqualTo("LongType");
		assertThat(actual.right).isNull();
	}

	@Test
	public void should_determine_composite_type_alias_for_composite_partition_key_creation() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.partitionClasses(Long.class, String.class).build();

		Pair<String, String> actual = factory.determineKeyValidationAndAlias(idMeta, true);

		assertThat(actual.left).isEqualTo("CompositeType");
		assertThat(actual.right).isEqualTo("(LongType,UTF8Type)");
	}
}
