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
package info.archinnov.achilles.iterator;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.composite.ThriftCompositeTransformer;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.BeanWithClusteredId;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftAbstractClusteredEntityIteratorTest {

	@Mock(answer = Answers.CALLS_REAL_METHODS)
	private ThriftAbstractClusteredEntityIterator<BeanWithClusteredId> abstractIter;

	@Mock
	private Iterator<BeanWithClusteredId> iterator;

	@Mock
	private ThriftPersistenceContext context;

	@Mock
	private ThriftCompositeTransformer transformer;

	@Mock
	private ThriftEntityProxifier proxifier;

	@Captor
	private ArgumentCaptor<Set<Method>> methodCaptor;

	private BeanWithClusteredId target = new BeanWithClusteredId();

	@Before
	public void setUp() {
		Whitebox.setInternalState(abstractIter, ThriftCompositeTransformer.class, transformer);
		Whitebox.setInternalState(abstractIter, ThriftEntityProxifier.class, proxifier);
		Whitebox.setInternalState(abstractIter, ThriftPersistenceContext.class, context);
		Whitebox.setInternalState(abstractIter, Iterator.class, iterator);
	}

	@Test
	public void should_return_true_for_has_next() throws Exception {
		when(iterator.hasNext()).thenReturn(true);
		assertThat(abstractIter.hasNext()).isTrue();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void should_exception_when_remove_called() throws Exception {
		abstractIter.remove();
	}

	@Test
	public void should_proxify_entity() throws Exception {
		Method idGetter = BeanWithClusteredId.class.getDeclaredMethod("getId");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();
		idMeta.setGetter(idGetter);
		when((PropertyMeta) context.getFirstMeta()).thenReturn(idMeta);
		when(context.isValueless()).thenReturn(false);
		when(context.duplicate(target)).thenReturn(context);
		when(proxifier.buildProxy(eq(target), eq(context), methodCaptor.capture())).thenReturn(target);

		BeanWithClusteredId actual = abstractIter.proxifyClusteredEntity(target);

		assertThat(actual).isSameAs(target);

		assertThat(methodCaptor.getValue()).containsOnly(idGetter);
	}

	@Test
	public void should_proxify_value_less_entity() throws Exception {
		Method idGetter = BeanWithClusteredId.class.getDeclaredMethod("getId");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();
		idMeta.setGetter(idGetter);
		when((PropertyMeta) context.getFirstMeta()).thenReturn(idMeta);
		when(context.isValueless()).thenReturn(true);
		when(context.duplicate(target)).thenReturn(context);
		when(proxifier.buildProxy(eq(target), eq(context), methodCaptor.capture())).thenReturn(target);

		BeanWithClusteredId actual = abstractIter.proxifyClusteredEntity(target);

		assertThat(actual).isSameAs(target);

		assertThat(methodCaptor.getValue()).isEmpty();
	}
}
