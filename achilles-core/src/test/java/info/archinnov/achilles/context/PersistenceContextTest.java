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
package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.FlushContext.FlushType;
import info.archinnov.achilles.entity.metadata.EntityMeta;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceContextTest {
	@Mock
	private PersistenceContext context;

	@Mock
	private EntityMeta entityMeta;

	@Mock
	private FlushContext<?> flushContext;

	@Before
	public void setUp() {
		doCallRealMethod().when(context).setEntityMeta(any(EntityMeta.class));
		doCallRealMethod().when(context).setFlushContext(
				any(FlushContext.class));

		context.setEntityMeta(entityMeta);
		context.setFlushContext(flushContext);
	}

	@Test
	public void should_return_wide_row() throws Exception {
		doCallRealMethod().when(context).isClusteredEntity();

		when(entityMeta.isClusteredEntity()).thenReturn(true);
		assertThat(context.isClusteredEntity()).isTrue();
	}

	@Test
	public void should_return_column_family_name() throws Exception {
		doCallRealMethod().when(context).getTableName();

		when(entityMeta.getTableName()).thenReturn("table");
		assertThat(context.getTableName()).isEqualTo("table");
	}

	@Test
	public void should_return_true_for_is_batch_mode() throws Exception {
		doCallRealMethod().when(context).isBatchMode();

		when(flushContext.type()).thenReturn(FlushType.BATCH);
		assertThat(context.isBatchMode()).isTrue();
	}

	@Test
	public void should_return_false_for_is_batch_mode() throws Exception {
		doCallRealMethod().when(context).isBatchMode();

		when(flushContext.type()).thenReturn(FlushType.IMMEDIATE);
		assertThat(context.isBatchMode()).isFalse();
	}

	@Test
	public void should_call_flush() throws Exception {
		doCallRealMethod().when(context).flush();

		context.flush();

		verify(flushContext).flush();
	}

	@Test
	public void should_call_end_batch() throws Exception {
		doCallRealMethod().when(context).endBatch();

		context.endBatch();

		verify(flushContext).endBatch();
	}
}
