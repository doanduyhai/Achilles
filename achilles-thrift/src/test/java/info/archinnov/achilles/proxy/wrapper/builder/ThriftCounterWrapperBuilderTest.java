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
package info.archinnov.achilles.proxy.wrapper.builder;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.proxy.wrapper.ThriftCounterWrapper;
import info.archinnov.achilles.type.ConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftCounterWrapperBuilderTest {

	private Long key = RandomUtils.nextLong();

	@Mock
	private Composite columnName;

	@Mock
	private ThriftAbstractDao counterDao;

	@Mock
	private ThriftPersistenceContext context;

	private ConsistencyLevel consistencyLevel = ConsistencyLevel.ALL;

	@Test
	public void should_build() throws Exception {
		ThriftCounterWrapper built = ThriftCounterWrapperBuilder
				.builder(context)
				//
				.columnName(columnName).key(key).counterDao(counterDao)
				.consistencyLevel(consistencyLevel).build();

		assertThat(Whitebox.getInternalState(built, "key")).isSameAs(key);
		assertThat(Whitebox.getInternalState(built, "columnName")).isSameAs(
				columnName);
		assertThat(Whitebox.getInternalState(built, "context")).isSameAs(
				context);
		assertThat(Whitebox.getInternalState(built, "counterDao")).isSameAs(
				counterDao);
		assertThat(Whitebox.getInternalState(built, "consistencyLevel"))
				.isSameAs(consistencyLevel);
	}
}
