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

import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ValuelessEntity;
import info.archinnov.achilles.options.OptionsBuilder;

public class ValuelessEntityIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, ValuelessEntity.TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_persist_and_find() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		ValuelessEntity entity = new ValuelessEntity(id);

		manager.insert(entity);

		ValuelessEntity found = manager.find(ValuelessEntity.class, id);

		assertThat(found).isNotNull();
	}

	@Test
	public void should_persist_and_get_proxy() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		ValuelessEntity entity = new ValuelessEntity(id);

		manager.insert(entity);

		ValuelessEntity found = manager.getProxy(ValuelessEntity.class, id);

		assertThat(found).isNotNull();
	}

	@Test
	public void should_persist_with_ttl() throws Exception {
		Long id = RandomUtils.nextLong(0,Long.MAX_VALUE);
		ValuelessEntity entity = new ValuelessEntity(id);

		manager.insert(entity, OptionsBuilder.withTtl(1));

		Thread.sleep(1000);

		assertThat(manager.find(ValuelessEntity.class, id)).isNull();
	}

}
