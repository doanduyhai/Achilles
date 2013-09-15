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
package info.archinnov.achilles.composite;

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.type.BoundingMode.EXCLUSIVE_BOUNDS;
import static info.archinnov.achilles.type.OrderingMode.DESCENDING;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompositeFactoryTest {
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@InjectMocks
	private ThriftCompositeFactory factory;

	@Mock
	private ComponentEqualityCalculator calculator;

	@Mock
	private ThriftCompoundKeyMapper compoundKeyMapper;

	@Mock
	private CompoundKeyValidator compoundKeyValidator;

	@Mock
	private PropertyMeta embeddedIdMeta;

	@Mock
	private DataTranscoder transcoder;

	@Before
	public void setUp() {

		when(embeddedIdMeta.isEmbeddedId()).thenReturn(true);
		when(embeddedIdMeta.getPropertyName()).thenReturn("property");
	}

	@Test
	public void should_create_for_embedded_id_insert() throws Exception {
		TweetCompoundKey tweetKey = new TweetCompoundKey();
		Composite comp = new Composite();

		when(
				compoundKeyMapper.fromCompoundToCompositeForInsertOrGet(
						tweetKey, embeddedIdMeta)).thenReturn(comp);

		Composite actual = factory.createCompositeForClustered(embeddedIdMeta,
				tweetKey);

		assertThat(actual).isSameAs(comp);
	}

	@Test
	public void should_create_key_for_counter() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(Long.class)
				.transcoder(transcoder).build();

		when(transcoder.forceEncodeToJSON(11L)).thenReturn("11");

		Composite comp = factory.createKeyForCounter("fqcn", 11L, idMeta);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("fqcn");
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("11");
	}

	@Test
	public void should_create_base_for_get() throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createBaseForGet(meta);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(
				SIMPLE.flag());
		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(1).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(2).getValue(STRING_SRZ)).isEqualTo("0");
		assertThat(comp.getComponent(2).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_create_base_for_clustered_get() throws Exception {
		Object compoundKey = new CompoundKey();
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).type(SIMPLE).build();

		Composite comp = new Composite();
		when(
				compoundKeyMapper.fromCompoundToCompositeForInsertOrGet(
						compoundKey, idMeta)).thenReturn(comp);
		Composite actual = factory.createBaseForClusteredGet(compoundKey,
				idMeta);

		assertThat(actual).isSameAs(comp);
	}

	@Test
	public void should_create_base_for_counter_get() throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createBaseForCounterGet(meta);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
	}

	@Test
	public void should_create_base_for_query() throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createBaseForQuery(meta, GREATER_THAN_EQUAL);

		assertThat(comp.getComponents()).hasSize(2);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(
				SIMPLE.flag());
		assertThat(comp.getComponent(0).getEquality()).isEqualTo(EQUAL);
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(1).getEquality()).isEqualTo(
				GREATER_THAN_EQUAL);
	}

	@Test
	public void should_create_for_batch_insert_single() throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createForBatchInsertSingleValue(meta);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(
				SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue(STRING_SRZ)).isEqualTo("0");
	}

	@Test
	public void should_create_for_batch_insert_single_counter()
			throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createForBatchInsertSingleCounter(meta);

		assertThat(comp.getComponents()).hasSize(1);
		assertThat(comp.getComponent(0).getValue(STRING_SRZ)).isEqualTo("name");
	}

	@Test
	public void should_create_for_batch_insert_list_value() throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createForBatchInsertList(meta, 21);

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(
				SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue(STRING_SRZ)).isEqualTo(
				"000021");
	}

	@Test
	public void should_create_for_batch_insert_set_map_value() throws Exception {
		PropertyMeta meta = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name").build();

		Composite comp = factory.createForBatchInsertSetOrMap(meta, "text");

		assertThat(comp.getComponents()).hasSize(3);
		assertThat(comp.getComponent(0).getValue(BYTE_SRZ)).isEqualTo(
				SIMPLE.flag());
		assertThat(comp.getComponent(1).getValue(STRING_SRZ)).isEqualTo("name");
		assertThat(comp.getComponent(2).getValue(STRING_SRZ)).isEqualTo("text");
	}

	@Test
	public void should_create_for_clustered_query() throws Exception {

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(Long.class)
				.type(SIMPLE).field("name")
				.compClasses(Long.class, String.class).build();

		List<Object> clusteringFrom = Arrays.<Object> asList(11L, "z");
		List<Object> clusteringTo = Arrays.<Object> asList(11L, "a");
		Composite from = new Composite(), to = new Composite();

		when(calculator.determineEquality(EXCLUSIVE_BOUNDS, DESCENDING))
				.thenReturn(
						new ComponentEquality[] { LESS_THAN_EQUAL,
								GREATER_THAN_EQUAL });

		when(
				compoundKeyMapper.fromComponentsToCompositeForQuery(
						clusteringFrom, pm, LESS_THAN_EQUAL)).thenReturn(from);

		when(
				compoundKeyMapper.fromComponentsToCompositeForQuery(
						clusteringTo, pm, GREATER_THAN_EQUAL)).thenReturn(to);

		Composite[] composites = factory.createForClusteredQuery(pm,
				clusteringFrom, clusteringTo, EXCLUSIVE_BOUNDS, DESCENDING);

		assertThat(composites[0]).isSameAs(from);
		assertThat(composites[1]).isSameAs(to);

		verify(compoundKeyValidator).validateComponentsForSliceQuery(pm,
				clusteringFrom, clusteringTo, DESCENDING);

	}
}
