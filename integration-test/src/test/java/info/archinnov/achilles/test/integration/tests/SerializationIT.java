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

package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder.builder;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.type.CounterBuilder;
import net.sf.cglib.proxy.Factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;

public class SerializationIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean");

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_serialized_proxified_entity_with_Jackson() throws Exception {

		CompleteBean bean = builder().randomId().name("DuyHai").version(CounterBuilder.incr(2L)).buid();
		CompleteBean managedBean = manager.persist(bean);

		ObjectMapper mapper = new ObjectMapper();
		String serialized = mapper.writeValueAsString(managedBean);

		assertThat(serialized).doesNotContain("allback");

		CompleteBean deserialized = mapper.readValue(serialized, CompleteBean.class);

		assertThat(deserialized).isNotInstanceOf(Factory.class);
		assertThat(deserialized.getId()).isEqualTo(bean.getId());
		assertThat(deserialized.getName()).isEqualTo(bean.getName());
		assertThat(deserialized.getVersion().get()).isEqualTo(bean.getVersion().get());
	}
}
