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
package info.archinnov.achilles.test.integration.tests.bugs;

import com.datastax.driver.core.Session;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.logger.AchillesLoggers;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter;
import info.archinnov.achilles.test.integration.utils.CassandraLogAsserter;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter.CompoundPK;
import static info.archinnov.achilles.test.integration.entity.ClusteredEntityWithCounter.TABLE_NAME;
import static info.archinnov.achilles.type.CounterBuilder.incr;
import static org.fest.assertions.api.Assertions.assertThat;

public class LoadAllValuesAtOnceForClusteredCountersIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

	private ClusteredEntityWithCounter entity;

	private CompoundPK compoundKey;

	CassandraLogAsserter logAsserter = new CassandraLogAsserter();

	@Test
	public void should_persist_and_find() throws Exception {
		long counterValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		long versionValue = RandomUtils.nextLong(0,Long.MAX_VALUE);
		compoundKey = new CompoundPK(RandomUtils.nextLong(0,Long.MAX_VALUE), "name");

		entity = new ClusteredEntityWithCounter(compoundKey, incr(counterValue), incr(versionValue));

		manager.insert(entity);

		ClusteredEntityWithCounter found = manager.find(ClusteredEntityWithCounter.class, compoundKey);

		assertThat(found.getId()).isEqualTo(compoundKey);


		logAsserter.prepareLogLevel(AchillesLoggers.ACHILLES_DML_STATEMENT);

		assertThat(found.getCounter().get()).isEqualTo(counterValue);
		assertThat(found.getVersion().get()).isEqualTo(versionValue);

		logAsserter.assertNotContains("SELECT");
	}

}
