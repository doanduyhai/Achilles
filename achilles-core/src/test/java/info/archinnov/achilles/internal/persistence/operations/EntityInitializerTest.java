/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.persistence.operations;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityInitializerTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private EntityInitializer initializer = new EntityInitializer();

	@Mock
	private EntityMeta meta;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private PropertyMeta counterMeta;

	private CompleteBean bean = new CompleteBean();

	@Test
	public void should_initialize_and_set_counter_value_for_entity() throws Exception {

		when(meta.getAllCounterMetas()).thenReturn(Arrays.asList(counterMeta));

		initializer.initializeEntity(bean, meta);

		verify(counterMeta.forValues()).forceLoad(bean);

	}

}
