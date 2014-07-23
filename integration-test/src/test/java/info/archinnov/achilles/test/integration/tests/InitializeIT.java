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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.cassandra.utils.UUIDGen;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.RegularStatement;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterImpl;
import info.archinnov.achilles.persistence.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.Tweet;
import info.archinnov.achilles.type.CounterBuilder;

public class InitializeIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "CompleteBean",
			"Tweet");

	private PersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_initialize_lazy_properties() throws Exception {
		Tweet tweet = new Tweet();
		tweet.setId(UUIDGen.getTimeUUID());
		tweet.setContent("welcome");

		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").label("label").age(45L)
				.addFriends("foo", "bar").welcomeTweet(tweet).version(CounterBuilder.incr(11L)).buid();

		manager.insert(entity);

		CompleteBean foundEntity = manager.find(CompleteBean.class, entity.getId());

		CompleteBean rawEntity = manager.initAndRemoveProxy(foundEntity);

		assertThat(rawEntity.getName()).isEqualTo("name");
		assertThat(rawEntity.getLabel()).isEqualTo("label");
		assertThat(rawEntity.getAge()).isEqualTo(45L);
		assertThat(rawEntity.getFriends()).containsExactly("foo", "bar");
		assertThat(rawEntity.getWelcomeTweet().getContent()).isEqualTo("welcome");
		assertThat(rawEntity.getVersion()).isInstanceOf(InternalCounterImpl.class);
		assertThat(rawEntity.getVersion().get()).isEqualTo(11L);
	}

	@Test
	public void should_initialize_counter_value() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId().name("name").buid();

		entity = manager.insert(entity);

		entity.getVersion().incr(2L);

        manager.update(entity);

        RegularStatement statement = select().from("CompleteBean").where(eq("id",bindMarker()));

		CompleteBean foundEntity = manager.typedQuery(CompleteBean.class,statement,entity.getId()).getFirst();

		CompleteBean rawEntity = manager.initAndRemoveProxy(foundEntity);

		assertThat(rawEntity.getVersion()).isInstanceOf(InternalCounterImpl.class);
		assertThat(rawEntity.getVersion().get()).isEqualTo(2L);
	}
}
