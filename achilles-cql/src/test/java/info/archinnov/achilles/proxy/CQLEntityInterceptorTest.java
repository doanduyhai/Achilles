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
package info.archinnov.achilles.proxy;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.mapping.entity.ClusteredEntityWithCounter;
import info.archinnov.achilles.type.Counter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class CQLEntityInterceptorTest {

	private CQLEntityInterceptor<ClusteredEntityWithCounter> interceptor;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private CQLPersistenceContext context;

	@Test
	public void should_build_counter_wrapper() throws Exception {
		PropertyMeta counterMeta = new PropertyMeta();

		interceptor = new CQLEntityInterceptor<ClusteredEntityWithCounter>();
		interceptor.setContext(context);

		when(context.getEntityMeta().isClusteredCounter()).thenReturn(false);
		Counter counterWrapper = interceptor.buildCounterWrapper(counterMeta);

		assertThat(counterWrapper).isNotNull();
		assertThat(Whitebox.getInternalState(counterWrapper, CQLPersistenceContext.class)).isSameAs(context);
		assertThat(Whitebox.getInternalState(counterWrapper, PropertyMeta.class)).isSameAs(counterMeta);
	}
}
