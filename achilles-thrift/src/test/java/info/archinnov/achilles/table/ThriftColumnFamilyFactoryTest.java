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
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.table.ThriftColumnFamilyFactory.*;
import static me.prettyprint.hector.api.ddl.ComparatorType.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.cassandra.utils.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ThriftColumnFamilyFactoryTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftColumnFamilyFactory factory;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private PropertyMeta idMeta;

	@Mock
	private PropertyMeta propertyMeta;

	@Mock
	private Keyspace keyspace;

	@Mock
	private ThriftComparatorTypeAliasFactory comparatorAliasFactory;

	@Mock
	private ColumnFamilyDefinition cfDef;

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_entity_column_family() throws Exception {
		PropertyMeta propertyMeta = mock(PropertyMeta.class);
		when((Class<String>) propertyMeta.getValueClass()).thenReturn(String.class);

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("age", propertyMeta);

		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(entityMeta.getTableName()).thenReturn("myCF");
		when(entityMeta.getClassName()).thenReturn("fr.doan.test.bean");
		when(idMeta.isCompositePartitionKey()).thenReturn(false);
		when(idMeta.isEmbeddedId()).thenReturn(false);
		when((Class<Long>) idMeta.getValueClass()).thenReturn(Long.class);
		when(comparatorAliasFactory.determineKeyValidationAndAlias(idMeta, true)).thenReturn(
				Pair.<String, String> create(LONG_SRZ.getComparatorType().getTypeName(), null));
		ColumnFamilyDefinition cfDef = factory.createEntityCF(entityMeta, "keyspace");

		assertThat(cfDef).isNotNull();
		assertThat(cfDef.getKeyspaceName()).isEqualTo("keyspace");
		assertThat(cfDef.getName()).isEqualTo("myCF");
		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(LONG_SRZ.getComparatorType().getTypeName());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_create_entity_column_family_with_composite_row_key() throws Exception {
		PropertyMeta propertyMeta = mock(PropertyMeta.class);
		when((Class<String>) propertyMeta.getValueClass()).thenReturn(String.class);

		Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
		propertyMetas.put("age", propertyMeta);

		when(entityMeta.hasCompositePartitionKey()).thenReturn(true);
		when(entityMeta.getPropertyMetas()).thenReturn(propertyMetas);
		when(entityMeta.getIdMeta()).thenReturn(idMeta);
		when(idMeta.isCompositePartitionKey()).thenReturn(true);
		when(idMeta.isEmbeddedId()).thenReturn(true);
		when(comparatorAliasFactory.determineKeyValidationAndAlias(idMeta, true)).thenReturn(
				Pair.create(COMPOSITE_SRZ.getComparatorType().getTypeName(), "alias"));
		when(entityMeta.getTableName()).thenReturn("myCF");
		when(entityMeta.getClassName()).thenReturn("fr.doan.test.bean");

		ColumnFamilyDefinition cfDef = factory.createEntityCF(entityMeta, "keyspace");

		assertThat(cfDef).isNotNull();
		assertThat(cfDef.getKeyspaceName()).isEqualTo("keyspace");
		assertThat(cfDef.getName()).isEqualTo("myCF");
		assertThat(cfDef.getComparatorType()).isEqualTo(ComparatorType.COMPOSITETYPE);
		assertThat(cfDef.getKeyValidationClass()).isEqualTo(COMPOSITE_SRZ.getComparatorType().getTypeName());
		assertThat(cfDef.getKeyValidationAlias()).isEqualTo("alias");
	}

	@Test
	public void should_create_clustered_entity_column_family() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
				.compClasses(Long.class, String.class, UUID.class).field("id").type(EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, Integer.class).field("name")
				.type(PropertyType.SIMPLE).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("tableName");
		meta.setClassName("entityName");
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "name", pm));
		meta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		meta.setFirstMeta(pm);
		when(comparatorAliasFactory.determineKeyValidationAndAlias(idMeta, true)).thenReturn(
				Pair.<String, String> create(LONG_SRZ.getComparatorType().getTypeName(), null));
		when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteringComponents(idMeta, true)).thenReturn(
				"(UTF8Type,UUIDType)");
		ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

		assertThat(cfDef.getKeyValidationClass()).isEqualTo(LONG_SRZ.getComparatorType().getTypeName());
		assertThat(cfDef.getComparatorType()).isEqualTo(COMPOSITE_SRZ.getComparatorType());
		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("(UTF8Type,UUIDType)");
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(INT_SRZ.getComparatorType().getTypeName());
	}

	@Test
	public void should_create_clustered_entity_column_family_with_composite_row_key() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
				.partitionClasses(Long.class, String.class).clusteringClasses(UUID.class).field("id").type(EMBEDDED_ID)
				.build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, Integer.class).field("name").type(SIMPLE)
				.build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("tableName");
		meta.setClassName("entityName");
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "name", pm));
		meta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		meta.setFirstMeta(pm);

		when(comparatorAliasFactory.determineKeyValidationAndAlias(idMeta, true)).thenReturn(
				Pair.create(COMPOSITE_SRZ.getComparatorType().getTypeName(), "(LongType,UTF8Type)"));
		when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteringComponents(idMeta, true)).thenReturn(
				"(UUIDType)");
		ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

		assertThat(cfDef.getKeyValidationClass()).isEqualTo(COMPOSITE_SRZ.getComparatorType().getTypeName());
		assertThat(cfDef.getKeyValidationAlias()).isEqualTo("(LongType,UTF8Type)");
		assertThat(cfDef.getComparatorType()).isEqualTo(COMPOSITE_SRZ.getComparatorType());
		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("(UUIDType)");
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(INT_SRZ.getComparatorType().getTypeName());
	}

	@Test
	public void should_create_clustered_entity_column_family_with_counter_clustered_value() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
				.compClasses(Long.class, String.class, UUID.class).field("id").type(PropertyType.EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, Long.class).field("name")
				.type(PropertyType.COUNTER).build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setTableName("tableName");
		meta.setClassName("entityName");
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta, "name", pm));
		meta.setAllMetasExceptIdMeta(Arrays.asList(pm));
		meta.setFirstMeta(pm);
		when(comparatorAliasFactory.determineKeyValidationAndAlias(idMeta, true)).thenReturn(
				Pair.<String, String> create(LONG_SRZ.getComparatorType().getTypeName(), null));
		when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteringComponents(idMeta, true)).thenReturn(
				"(UTF8Type,UUIDType)");
		ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(COUNTERTYPE.getTypeName());
	}

	@Test
	public void should_create_value_less_clustered_entity_column_family() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class, UUID.class).build();

		EntityMeta meta = new EntityMeta();
		meta.setTableName("tableName");
		meta.setClassName("entityName");
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("id", idMeta));
		when(comparatorAliasFactory.determineKeyValidationAndAlias(idMeta, true)).thenReturn(
				Pair.<String, String> create(LONG_SRZ.getComparatorType().getTypeName(), null));
		when(comparatorAliasFactory.determineCompatatorTypeAliasForClusteringComponents(idMeta, true)).thenReturn(
				"(UTF8Type,UUIDType)");
		ColumnFamilyDefinition cfDef = factory.createClusteredEntityCF("keyspaceName", meta);

		assertThat(cfDef.getKeyValidationClass()).isEqualTo(LONG_SRZ.getComparatorType().getTypeName());
		assertThat(cfDef.getComparatorType()).isEqualTo(COMPOSITE_SRZ.getComparatorType());
		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo("(UTF8Type,UUIDType)");
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(STRING_SRZ.getComparatorType().getTypeName());
	}

	@Test
	public void should_create_counter_column_family() throws Exception {

		ColumnFamilyDefinition cfDef = factory.createCounterCF("keyspace");

		assertThat(cfDef.getKeyValidationClass()).isEqualTo(COMPOSITETYPE.getTypeName());
		assertThat(cfDef.getKeyValidationAlias()).isEqualTo(COUNTER_KEY_ALIAS);
		assertThat(cfDef.getComparatorType()).isEqualTo(COMPOSITETYPE);
		assertThat(cfDef.getComparatorTypeAlias()).isEqualTo(COUNTER_COMPARATOR_TYPE_ALIAS);
		assertThat(cfDef.getDefaultValidationClass()).isEqualTo(COUNTERTYPE.getClassName());

	}
}
