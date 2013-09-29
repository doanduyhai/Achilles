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
package info.archinnov.achilles.query;

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SliceQueryTest {

	@Mock
	private DataTranscoder transcoder;

	@Test
	public void should_build_new_slice_query() throws Exception {
		PropertyMeta idMeta = mock(PropertyMeta.class);

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);

		List<Object> fromComponents = Arrays.<Object> asList(11L, "a");
		List<Object> toComponents = Arrays.<Object> asList(11L, "b");
		when(idMeta.encodeToComponents(fromComponents)).thenReturn(fromComponents);
		when(idMeta.encodeToComponents(toComponents)).thenReturn(toComponents);

		SliceQuery<ClusteredEntity> sliceQuery = new SliceQuery<ClusteredEntity>(ClusteredEntity.class, meta,
				Arrays.<Object> asList(11L), Arrays.<Object> asList("a"), Arrays.<Object> asList("b"),
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 99, false);

		assertThat(sliceQuery.getEntityClass()).isSameAs(ClusteredEntity.class);
		assertThat(sliceQuery.getBatchSize()).isEqualTo(99);
		assertThat(sliceQuery.getBounding()).isEqualTo(BoundingMode.INCLUSIVE_BOUNDS);
		assertThat(sliceQuery.getClusteringsFrom()).containsExactly(11L, "a");
		assertThat(sliceQuery.getClusteringsTo()).containsExactly(11L, "b");
		assertThat(sliceQuery.getConsistencyLevel()).isNull();
		assertThat(sliceQuery.getLimit()).isEqualTo(100);
		assertThat(sliceQuery.getMeta()).isSameAs(meta);
		assertThat(sliceQuery.getOrdering()).isSameAs(OrderingMode.ASCENDING);
		assertThat(sliceQuery.getPartitionComponents()).containsExactly(11L);
		assertThat(sliceQuery.isLimitSet()).isFalse();
	}

	@Test
	public void should_return_true_when_no_component() throws Exception {
		PropertyMeta idMeta = mock(PropertyMeta.class);

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		SliceQuery<ClusteredEntity> sliceQuery = new SliceQuery<ClusteredEntity>(ClusteredEntity.class, meta,
				Arrays.<Object> asList(11L), Arrays.<Object> asList(), Arrays.<Object> asList(),
				OrderingMode.ASCENDING, BoundingMode.INCLUSIVE_BOUNDS, null, 100, 99, false);

		assertThat(sliceQuery.hasNoComponent()).isTrue();
	}
}
