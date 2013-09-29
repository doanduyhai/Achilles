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

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.test.builders.CompositeTestBuilder;
import info.archinnov.achilles.test.builders.HColumnTestBuilder;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.Component;
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
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.base.Function;
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
	private ThriftEntityMapper mapper;

	@Mock
	private ThriftPersistenceContext context;

	@Mock
	private DataTranscoder transcoder;

	@Mock
	private EntityMeta entityMeta;

	@Before
	public void setUp() {
		Whitebox.setInternalState(transformer, ThriftCompoundKeyMapper.class, compoundKeyMapper);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_build_raw_value_transformer() throws Exception {
		Composite comp1 = CompositeTestBuilder.builder().buildSimple();
		Composite comp2 = CompositeTestBuilder.builder().buildSimple();
		HColumn<Composite, String> hCol1 = HColumnTestBuilder.simple(comp1, "test1");
		HColumn<Composite, String> hCol2 = HColumnTestBuilder.simple(comp2, "test2");

		List<Object> rawValues = Lists.transform(Arrays.asList(hCol1, hCol2), transformer.buildRawValueTransformer());

		assertThat(rawValues).containsExactly("test1", "test2");
	}

	@Test
	public void should_build_clustered_entity_transformer() throws Exception {
		Object primaryKey = RandomUtils.nextLong();
		BeanWithClusteredId expected = new BeanWithClusteredId();
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		String clusteredValue = "value";
		EmbeddedKey embeddedKey = new EmbeddedKey();
		HColumn<Composite, Object> hCol1 = HColumnTestBuilder.<Object> simple(comp1, clusteredValue);
		List<Component<?>> components = hCol1.getName().getComponents();
		List<HColumn<Composite, Object>> collection = Arrays.asList(hCol1);

		PropertyMeta idMeta = mock(PropertyMeta.class);
		PropertyMeta pm = mock(PropertyMeta.class);

		when(context.getEntityMeta()).thenReturn(entityMeta);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(context.getFirstMeta()).thenReturn(pm);
		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(compoundKeyMapper.fromCompositeToEmbeddedId(idMeta, components, primaryKey)).thenReturn(embeddedKey);
		when(pm.decode(clusteredValue)).thenReturn(clusteredValue);
		when(
				mapper.createClusteredEntityWithValue(BeanWithClusteredId.class, entityMeta, pm, embeddedKey,
						clusteredValue)).thenReturn(expected);

		Function<HColumn<Composite, Object>, BeanWithClusteredId> function = transformer.clusteredEntityTransformer(
				BeanWithClusteredId.class, context);

		List<BeanWithClusteredId> actualList = Lists.transform(collection, function);

		assertThat(actualList).hasSize(1);
		BeanWithClusteredId actual = actualList.get(0);

		assertThat(actual).isSameAs(expected);

	}

	@Test
	public void should_build_counter_clustered_entity_transformer() throws Exception {
		Object primaryKey = RandomUtils.nextLong();
		BeanWithClusteredId expected = new BeanWithClusteredId();
		EmbeddedKey embeddedKey = new EmbeddedKey();
		Composite comp1 = CompositeTestBuilder.builder().values(11).buildSimple();
		HCounterColumn<Composite> hCol1 = HColumnTestBuilder.counter(comp1, 150L);
		List<HCounterColumn<Composite>> collection = Arrays.asList(hCol1);

		PropertyMeta idMeta = mock(PropertyMeta.class);

		when(context.getPrimaryKey()).thenReturn(primaryKey);
		when(context.getIdMeta()).thenReturn(idMeta);
		when(context.getEntityMeta()).thenReturn(entityMeta);

		when(
				compoundKeyMapper.fromCompositeToEmbeddedId(eq(idMeta), Mockito.<List<Component<?>>> any(),
						eq(primaryKey))).thenReturn(embeddedKey);
		when(mapper.initClusteredEntity(BeanWithClusteredId.class, entityMeta, embeddedKey)).thenReturn(expected);

		Function<HCounterColumn<Composite>, BeanWithClusteredId> function = transformer
				.counterClusteredEntityTransformer(BeanWithClusteredId.class, context);

		List<BeanWithClusteredId> actualList = Lists.transform(collection, function);

		assertThat(actualList).hasSize(1);
		BeanWithClusteredId actual = actualList.get(0);

		assertThat(actual).isSameAs(expected);
	}
}
