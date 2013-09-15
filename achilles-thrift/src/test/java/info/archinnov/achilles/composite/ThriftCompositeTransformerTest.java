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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.test.builders.CompositeTestBuilder;
import info.archinnov.achilles.test.builders.HColumnTestBuilder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCompositeTransformerTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftCompositeTransformer transformer;

	@Mock
	private ThriftCompoundKeyMapper compoundKeyMapper;

	@Mock
	private ThriftEntityProxifier proxifier;

	@Mock
	private ThriftEntityMapper entityMapper;

	@Mock
	private ThriftPersistenceContext context;

	@Mock
	private ThriftPersistenceContext joinContext;

	@Mock
	private DataTranscoder transcoder;

	@Before
	public void setUp() {
		Whitebox.setInternalState(transformer, ThriftCompoundKeyMapper.class,
				compoundKeyMapper);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_raw_value_transformer() throws Exception {
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1,
				"test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2,
				"test2");

		List<Object> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2),
				transformer.buildRawValueTransformer());

		assertThat(rawValues).containsExactly("test1", "test2");
	}

	@Test
	public void should_build_clustered_entity_transformer() throws Exception {
		Object partitionKey = RandomUtils.nextLong();
		BeanWithClusteredId expected = new BeanWithClusteredId();
		Composite comp1 = CompositeTestBuilder.builder().values(11)
				.buildSimple();
		String clusteredValue = "value";
		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.<Object> simple(
				comp1, clusteredValue);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.keyValueClass(CompoundKey.class, CompoundKey.class)
				.type(EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.type(SIMPLE).accessors().build();

		when(context.getIdMeta()).thenReturn((PropertyMeta) idMeta);
		when(context.getFirstMeta()).thenReturn((PropertyMeta) pm);

		CompoundKey compoundKey = new CompoundKey();
		when(context.getPartitionKey()).thenReturn(partitionKey);
		when(
				compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta),
						any(List.class), eq(partitionKey))).thenReturn(
				compoundKey);

		when(
				entityMapper.createClusteredEntityWithValue(
						BeanWithClusteredId.class, idMeta, pm, compoundKey,
						clusteredValue)).thenReturn(expected);

		Function<HColumn<Composite, Object>, BeanWithClusteredId> function = transformer
				.clusteredEntityTransformer(BeanWithClusteredId.class, context);

		List<BeanWithClusteredId> actualList = Lists.transform(
				Arrays.asList(hCol1), function);

		assertThat(actualList).hasSize(1);
		BeanWithClusteredId actual = actualList.get(0);
		assertThat(actual).isSameAs(expected);

	}

	@Test
	public void should_build_counter_clustered_entity_transformer()
			throws Exception {
		Object partitionKey = RandomUtils.nextLong();
		BeanWithClusteredId expected = new BeanWithClusteredId();
		Composite comp1 = CompositeTestBuilder.builder().values(11)
				.buildSimple();
		HCounterColumn<Composite> hCol1 = HColumnTestBuilder.counter(comp1,
				150L);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.keyValueClass(CompoundKey.class, CompoundKey.class)
				.type(EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, Long.class).field("count")
				.type(PropertyType.COUNTER).accessors().build();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setPropertyMetas(ImmutableMap.of("name", pm));

		when(context.getEntityMeta()).thenReturn(meta);
		when(context.getIdMeta()).thenReturn((PropertyMeta) idMeta);

		CompoundKey compoundKey = new CompoundKey();
		when(context.getPartitionKey()).thenReturn(partitionKey);
		when(
				compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta),
						any(List.class), eq(partitionKey))).thenReturn(
				compoundKey);

		when(
				entityMapper.initClusteredEntity(BeanWithClusteredId.class,
						idMeta, compoundKey)).thenReturn(expected);

		Function<HCounterColumn<Composite>, BeanWithClusteredId> function = transformer
				.counterClusteredEntityTransformer(BeanWithClusteredId.class,
						context);

		List<BeanWithClusteredId> actualList = Lists.transform(
				Arrays.asList(hCol1), function);

		assertThat(actualList).hasSize(1);
		BeanWithClusteredId actual = actualList.get(0);
		assertThat(actual).isSameAs(expected);
	}

	@Test
	public void should_build_join_clustered_entity_transformer()
			throws Exception {
		Object partitionKey = RandomUtils.nextLong();

		BeanWithClusteredId expected = new BeanWithClusteredId();
		long joinId = 10L;
		UserBean joinEntity = new UserBean();
		Map<Object, Object> joinEntitiesMap = ImmutableMap.<Object, Object> of(
				joinId, joinEntity);

		Composite comp1 = CompositeTestBuilder.builder().values(11)
				.buildSimple();
		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.<Object> simple(
				comp1, joinId);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.keyValueClass(CompoundKey.class, CompoundKey.class)
				.type(EMBEDDED_ID).build();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, UserBean.class).field("user")
				.transcoder(transcoder).type(PropertyType.JOIN_SIMPLE)
				.accessors().build();

		when(context.getIdMeta()).thenReturn((PropertyMeta) idMeta);
		when(context.getFirstMeta()).thenReturn((PropertyMeta) pm);

		CompoundKey embeddedId = new CompoundKey();
		when(context.getPartitionKey()).thenReturn(partitionKey);
		when(
				compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta),
						any(List.class), eq(partitionKey))).thenReturn(
				embeddedId);

		when(
				entityMapper.createClusteredEntityWithValue(
						eq(BeanWithClusteredId.class), eq(idMeta), eq(pm),
						eq(embeddedId), any(UserBean.class))).thenReturn(
				expected);

		when(transcoder.decode(pm, expected)).thenReturn(expected);
		Function<HColumn<Composite, Object>, BeanWithClusteredId> function = transformer
				.joinClusteredEntityTransformer(BeanWithClusteredId.class,
						context, joinEntitiesMap);

		List<BeanWithClusteredId> actualList = Lists.transform(
				Arrays.asList(hCol1), function);

		assertThat(actualList).hasSize(1);
		BeanWithClusteredId actual = actualList.get(0);

		assertThat(actual).isSameAs(expected);
	}

}
