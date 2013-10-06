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
package info.archinnov.achilles.query.slice;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.operations.SliceQueryExecutor;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntity;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.IndexEquality;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryBuilderTest {

	private SliceQueryBuilder<PersistenceContext, ClusteredEntity> builder;

	@Mock
	private SliceQueryExecutor<PersistenceContext> sliceQueryExecutor;

	@Mock
	private CompoundKeyValidator compoundKeyValidator;

	@Mock
	private DataTranscoder transcoder;

	private EntityMeta meta;

	private PropertyMeta idMeta;

	@Before
	public void setUp() throws Exception {
		meta = new EntityMeta();
		meta.setIdClass(CompoundKey.class);

		Method userIdGetter = CompoundKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

		idMeta = PropertyMetaTestBuilder.valueClass(CompoundKey.class).compGetters(userIdGetter, nameGetter)
				.compClasses(Long.class, String.class).transcoder(transcoder).build();

		meta.setIdMeta(idMeta);

		builder = new SliceQueryBuilder<PersistenceContext, ClusteredEntity>(sliceQueryExecutor, compoundKeyValidator,
				ClusteredEntity.class, meta);
		Whitebox.setInternalState(builder, "meta", meta);
	}

	@Test
	public void should_set_partition_key_and_create_builder() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		SliceQueryBuilder<PersistenceContext, ClusteredEntity>.SliceShortcutQueryBuilder shortCutBuilder = builder
				.partitionKey(partitionKey);

		assertThat(shortCutBuilder).isNotNull();
	}

	@Test
	public void should_set_from_embedded_id_and_create_builder() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		String name = "name";
		CompoundKey compoundKey = new CompoundKey(partitionKey, name);

		List<Object> components = Arrays.<Object> asList(partitionKey, name);
		when(transcoder.encodeToComponents(idMeta, compoundKey)).thenReturn(components);

		SliceQueryBuilder<PersistenceContext, ClusteredEntity>.SliceFromEmbeddedIdBuilder embeddedIdBuilder = builder
				.fromEmbeddedId(compoundKey);

		assertThat(embeddedIdBuilder).isNotNull();
	}

	@Test
	public void should_set_to_embedded_id_and_create_builder() throws Exception {
		Long partitionKey = RandomUtils.nextLong();
		String name = "name";
		CompoundKey compoundKey = new CompoundKey(partitionKey, name);

		List<Object> components = Arrays.<Object> asList(partitionKey, name);
		when(transcoder.encodeToComponents(idMeta, compoundKey)).thenReturn(components);

		SliceQueryBuilder<PersistenceContext, ClusteredEntity>.SliceToEmbeddedIdBuilder embeddedIdBuilder = builder
				.toEmbeddedId(compoundKey);

		assertThat(embeddedIdBuilder).isNotNull();

	}

	@Test
	public void should_add_conditions_and_create_builder() throws Exception {
		Collection<IndexCondition> indexConditions = new LinkedList<IndexCondition>();
		indexConditions.add(new IndexCondition("test", IndexEquality.EQUAL, "value"));

		SliceQueryBuilder<PersistenceContext, ClusteredEntity>.DefaultQueryBuilder embeddedIdBuilder = builder
				.conditions(indexConditions, false);

		assertThat(embeddedIdBuilder).isNotNull();

	}

}
