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
package info.archinnov.achilles.compound;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.test.parser.entity.EmbeddedKeyWithEnum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompoundKeyMapperTest {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private ThriftCompoundKeyMapper mapper;

	@Mock
	private ThriftCompoundKeyValidator validator;

	@Mock
	private PropertyMeta compoundKeyMeta;

	@Mock
	private PropertyMeta compoundKeyWithEnumMeta;

	@Mock
	private PropertyMeta compoundKeyByConstructorMeta;

	@Mock
	private PropertyMeta compoundKeyByConstructorWithEnumMeta;

	@Mock
	private DataTranscoder transcoder;

	@Mock
	private ReflectionInvoker invoker;

	@Test
	public void should_build_embedded_id() throws Exception {
		Long id = RandomUtils.nextLong();
		List<Object> partitionComponents = Arrays.<Object> asList(id);
		String name = "name";
		EmbeddedKey embeddedKey = new EmbeddedKey();
		HColumn<Composite, String> hCol1 = buildHColumn(buildComposite(name), "val1");

		when(compoundKeyMeta.extractPartitionComponents(embeddedKey)).thenReturn(partitionComponents);
		when(compoundKeyMeta.getClusteringComponentClasses()).thenReturn(Arrays.<Class<?>> asList(String.class));
		when(compoundKeyMeta.decodeFromComponents(Arrays.<Object> asList(id, name))).thenReturn(embeddedKey);

		Object actual = mapper.fromCompositeToEmbeddedId(compoundKeyMeta, hCol1.getName().getComponents(), embeddedKey);

		assertThat(actual).isSameAs(embeddedKey);
	}

	@Test
	public void should_create_composite_for_embedded_id_insert() throws Exception {
		Long id = RandomUtils.nextLong();
		EmbeddedKeyWithEnum compoundKey = new EmbeddedKeyWithEnum();

		List<Object> componentValues = Arrays.<Object> asList(id, "EMBEDDED_ID");
		when(compoundKeyMeta.encodeToComponents(compoundKey)).thenReturn(componentValues);
		when(compoundKeyMeta.isEmbeddedId()).thenReturn(true);
		when(compoundKeyMeta.getClusteringComponentClasses()).thenReturn(Arrays.<Class<?>> asList(PropertyType.class));
		when(compoundKeyMeta.extractClusteringComponents(Mockito.<List<Object>> any())).thenReturn(
				Arrays.<Object> asList("EMBEDDED_ID"));

		Composite comp = mapper.fromCompoundToCompositeForInsertOrGet(compoundKey, compoundKeyMeta);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat(comp.getComponents().get(0).getValue(STRING_SRZ)).isEqualTo("EMBEDDED_ID");
	}

	@Test
	public void should_build_simple_row_key() throws Exception {
		Long id = RandomUtils.nextLong();
		ThriftPersistenceContext context = mock(ThriftPersistenceContext.class);

		when(context.getIdMeta()).thenReturn(compoundKeyMeta);
		when(context.getPrimaryKey()).thenReturn(id);
		when(compoundKeyMeta.isCompositePartitionKey()).thenReturn(false);

		Object actual = mapper.buildRowKey(context);

		assertThat(actual).isSameAs(id);
	}

	@Test
	public void should_build_composite_row_key() throws Exception {
		Long id = RandomUtils.nextLong();
		ThriftPersistenceContext context = mock(ThriftPersistenceContext.class);
		EmbeddedKey embeddedKey = new EmbeddedKey();
		UUID date = new UUID(10, 10);
		List<Object> components = Arrays.<Object> asList(id, "type", date);

		when(context.getIdMeta()).thenReturn(compoundKeyMeta);
		when(context.getPrimaryKey()).thenReturn(embeddedKey);
		when(compoundKeyMeta.isCompositePartitionKey()).thenReturn(true);
		when(compoundKeyMeta.encodeToComponents(embeddedKey)).thenReturn(components);
		when(compoundKeyMeta.extractPartitionComponents(components)).thenReturn(Arrays.<Object> asList(id, "type"));
		when(compoundKeyMeta.getPartitionComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(Long.class, String.class));

		Composite actual = (Composite) mapper.buildRowKey(context);

		assertThat(actual.getComponents()).hasSize(2);
		assertThat(actual.getComponents().get(0).getValue(LONG_SRZ)).isEqualTo(id);
		assertThat(actual.getComponents().get(1).getValue(STRING_SRZ)).isEqualTo("type");
	}

	@Test
	public void should_build_partition_key_from_compound_primary_key() throws Exception {
		Long id = RandomUtils.nextLong();
		ThriftPersistenceContext context = mock(ThriftPersistenceContext.class);
		EmbeddedKey embeddedKey = new EmbeddedKey();

		when(context.getIdMeta()).thenReturn(compoundKeyMeta);
		when(context.getPrimaryKey()).thenReturn(embeddedKey);
		when(compoundKeyMeta.isCompositePartitionKey()).thenReturn(false);
		when(compoundKeyMeta.isEmbeddedId()).thenReturn(true);
		when(compoundKeyMeta.getPartitionKey(embeddedKey)).thenReturn(id);

		Object actual = mapper.buildRowKey(context);

		assertThat(actual).isSameAs(id);
	}

	@Test
	public void should_exception_when_null_value() throws Exception {
		EmbeddedKeyWithEnum compoundKey = new EmbeddedKeyWithEnum();

		when(compoundKeyMeta.getPropertyName()).thenReturn("compound_key");
		List<Object> list = new ArrayList<Object>();
		list.add(null);
		when(compoundKeyMeta.encodeToComponents(compoundKey)).thenReturn(list);

		expectedEx.expect(AchillesException.class);
		expectedEx.expectMessage("The component values for the @EmbeddedId 'compound_key' should not be null");

		mapper.fromCompoundToCompositeForInsertOrGet(compoundKey, compoundKeyMeta);

	}

	@Test
	public void should_create_composite_from_components_for_query() throws Exception {
		UUID uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		List<Object> components = Arrays.<Object> asList(uuid, "a", 15);

		when(compoundKeyMeta.getPropertyName()).thenReturn("field");
		when(compoundKeyMeta.extractClusteringComponents(components)).thenReturn(Arrays.<Object> asList("a", 15));
		when(compoundKeyMeta.getClusteringComponentClasses()).thenReturn(
				Arrays.<Class<?>> asList(String.class, Integer.class));

		when(validator.validateNoHoleAndReturnLastNonNullIndex(Mockito.<List<Object>> any())).thenReturn(1);
		Composite comp = mapper.fromComponentsToCompositeForQuery(components, compoundKeyMeta, LESS_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);

		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(0).getValue()).isEqualTo("a");

		assertThat(comp.getComponent(1).getEquality()).isEqualTo(LESS_THAN_EQUAL);
		assertThat(comp.getComponent(1).getValue()).isEqualTo(15);
	}

	private Composite buildComposite(String name) {
		Composite composite = new Composite();
		composite.setComponent(0, name, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	private HColumn<Composite, String> buildHColumn(Composite comp, String value) {
		HColumn<Composite, String> hColumn = new HColumnImpl<Composite, String>(COMPOSITE_SRZ, STRING_SRZ);

		hColumn.setName(comp);
		hColumn.setValue(value);
		return hColumn;
	}

}
