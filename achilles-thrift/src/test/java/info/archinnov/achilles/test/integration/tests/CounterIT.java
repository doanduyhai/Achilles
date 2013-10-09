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
package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.counter.AchillesCounter.THRIFT_COUNTER_CF;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.STRING_SRZ;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.junit.AchillesInternalThriftResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CounterIT {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource("CompleteBean",
			THRIFT_COUNTER_CF);

	private ThriftPersistenceManager manager = resource.getPersistenceManager();

	private ThriftCounterDao counterDao = resource.getCounterDao();

	private CompleteBean bean;

	@Test
	public void should_persist_counter() throws Exception {
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.merge(bean);
		bean.getVersion().incr(2L);

		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		Composite comp = createCounterName("version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(2L);
	}

	@Test
	public void should_find_counter() throws Exception {
		long version = 10L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();

		bean = manager.merge(bean);
		bean.getVersion().incr(version);

		assertThat(bean.getVersion().get()).isEqualTo(version);
	}

	@Test
	public void should_remove_counter() throws Exception {
		long version = 154321L;
		bean = CompleteBeanTestBuilder.builder().randomId().name("test").buid();
		bean = manager.merge(bean);
		bean.getVersion().incr(version);
		Composite keyComp = createCounterKey(CompleteBean.class, bean.getId());
		Composite comp = createCounterName("version");
		Long actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isEqualTo(version);

		// Pause required to let Cassandra remove counter columns
		Thread.sleep(100);

		manager.remove(bean);

		actual = counterDao.getCounterValue(keyComp, comp);

		assertThat(actual).isNull();
	}

	private <T> Composite createCounterKey(Class<T> clazz, Long id) {
		Composite comp = new Composite();
		comp.setComponent(0, clazz.getCanonicalName(), STRING_SRZ);
		comp.setComponent(1, id.toString(), STRING_SRZ);
		return comp;
	}

	private Composite createCounterName(String propertyName) {
		Composite composite = new Composite();
		composite.addComponent(0, propertyName, ComponentEquality.EQUAL);
		return composite;
	}

}
