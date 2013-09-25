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

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.proxy.CQLEntityInterceptor;
import info.archinnov.achilles.proxy.wrapper.CounterBuilder;
import info.archinnov.achilles.test.integration.entity.ClusteredEntityWithTimeUUID;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.sf.cglib.proxy.Factory;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Session;

public class QueryIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(
			Steps.AFTER_TEST, "CompleteBean", AchillesCounter.CQL_COUNTER_TABLE);

	private CQLEntityManager em = resource.getEm();

	private Session session = resource.getNativeSession();

	@Test
	public void should_return_rows_for_native_query() throws Exception {
		CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(CounterBuilder.incr(15L)).buid();

		CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId()
				.name("John DOO").age(35L).addFriends("qux", "twix")
				.addFollowers("Isaac", "Lara").addPreference(1, "US")
				.addPreference(2, "NewYork").version(CounterBuilder.incr(17L))
				.buid();

		em.persist(entity1);
		em.persist(entity2);

		String nativeQuery = "SELECT name,age_in_years,friends,followers,preferences FROM CompleteBean WHERE id IN("
				+ entity1.getId() + "," + entity2.getId() + ")";

		List<Map<String, Object>> actual = em.nativeQuery(nativeQuery).get();

		assertThat(actual).hasSize(2);

		Map<String, Object> row1 = actual.get(0);
		Map<String, Object> row2 = actual.get(1);

		assertThat(row1.get("name")).isEqualTo("DuyHai");
		assertThat(row1.get("age_in_years")).isEqualTo(35L);
		assertThat((List<String>) row1.get("friends")).containsExactly("foo",
				"bar");
		assertThat((Set<String>) row1.get("followers")).contains("George",
				"Paul");
		Map<Integer, String> preferences1 = (Map<Integer, String>) row1
				.get("preferences");
		assertThat(preferences1.get(1)).isEqualTo("FR");
		assertThat(preferences1.get(2)).isEqualTo("Paris");
		assertThat(preferences1.get(3)).isEqualTo("75014");

		assertThat(row2.get("name")).isEqualTo("John DOO");
		assertThat(row2.get("age_in_years")).isEqualTo(35L);
		assertThat((List<String>) row2.get("friends")).containsExactly("qux",
				"twix");
		assertThat((Set<String>) row2.get("followers")).contains("Isaac",
				"Lara");
		Map<Integer, String> preferences2 = (Map<Integer, String>) row2
				.get("preferences");
		assertThat(preferences2.get(1)).isEqualTo("US");
		assertThat(preferences2.get(2)).isEqualTo("NewYork");
	}

	@Test
	public void should_return_count_for_native_query() throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").buid();

		em.persist(entity);

		Long count = (Long) em
				.nativeQuery(
						"SELECT COUNT(*) FROM CompleteBean WHERE id="
								+ entity.getId()).first().get("count");

		assertThat(count).isEqualTo(1L);
	}

	@Test
	public void should_return_ttl_and_timestamp_for_native_query()
			throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(32L).buid();

		Long timestamp = (System.currentTimeMillis() + 1234500) * 1000;

		em.persist(entity, OptionsBuilder.withTtl(1000).timestamp(timestamp));

		Map<String, Object> result = em.nativeQuery(
				"SELECT ttl(name),WRITETIME(age_in_years) FROM CompleteBean WHERE id="
						+ entity.getId()).first();

		assertThat((Integer) result.get("ttl(name)")).isLessThanOrEqualTo(1000);
		assertThat(result.get("writetime(age_in_years)")).isEqualTo(timestamp);
	}

	@Test
	public void should_return_cql_functions_for_native_query() throws Exception {

		Long id = RandomUtils.nextLong();
		UUID date = UUIDGen.getTimeUUID();

		em.persist(new ClusteredEntityWithTimeUUID(id, date, "value"));

		Map<String, Object> result = em
				.nativeQuery(
						"SELECT now(),dateOf(date),unixTimestampOf(date) FROM clustered_with_time_uuid WHERE id="
								+ id).first();
		assertThat(result.get("now()")).isNotNull().isInstanceOf(UUID.class);
		assertThat(result.get("dateOf(date)")).isNotNull().isInstanceOf(
				Date.class);
		assertThat(result.get("unixTimestampOf(date)")).isNotNull()
				.isInstanceOf(Long.class);
	}

	@Test
	public void should_return_entities_for_typed_query_with_select_star()
			throws Exception {
		Counter counter1 = CounterBuilder.incr(15L);
		CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(counter1).buid();

		Counter counter2 = CounterBuilder.incr(17L);
		CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId()
				.name("John DOO").age(34L).addFriends("qux", "twix")
				.addFollowers("Isaac", "Lara").addPreference(1, "US")
				.addPreference(2, "NewYork").version(counter2).buid();

		em.persist(entity1);
		em.persist(entity2);

		String queryString = "SELECT * FROM CompleteBean LIMIT 3";
		List<CompleteBean> actual = em.typedQuery(CompleteBean.class,
				queryString).get();

		assertThat(actual).hasSize(2);

		CompleteBean found1 = actual.get(0);
		CompleteBean found2 = actual.get(1);

		Factory factory1 = (Factory) found1;
		CQLEntityInterceptor<CompleteBean> interceptor1 = (CQLEntityInterceptor<CompleteBean>) factory1
				.getCallback(0);
		assertThat(interceptor1.getAlreadyLoaded()).hasSize(5);

		CompleteBean target1 = (CompleteBean) interceptor1.getTarget();

		assertThat(target1.getLabel()).isNull();
		assertThat(target1.getWelcomeTweet()).isNull();

		Factory factory2 = (Factory) found1;
		CQLEntityInterceptor<CompleteBean> interceptor2 = (CQLEntityInterceptor<CompleteBean>) factory2
				.getCallback(0);
		assertThat(interceptor2.getAlreadyLoaded()).hasSize(5);

		CompleteBean target2 = (CompleteBean) interceptor2.getTarget();

		assertThat(target2.getLabel()).isNull();
		assertThat(target2.getWelcomeTweet()).isNull();

		if (found1.getId().equals(entity1.getId())) {
			CompleteBean reference = entity1;
			Counter referenceCount = counter1;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isTrue();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getAge()).isEqualTo(reference.getAge());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found1.getPreferences().get(1)).isEqualTo("FR");
			assertThat(found1.getPreferences().get(2)).isEqualTo("Paris");
			assertThat(found1.getPreferences().get(3)).isEqualTo("75014");
			assertThat(found1.getVersion().get()).isEqualTo(
					referenceCount.get());

			reference = entity2;
			referenceCount = counter2;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isTrue();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getAge()).isEqualTo(reference.getAge());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found2.getPreferences().get(1)).isEqualTo("US");
			assertThat(found2.getPreferences().get(2)).isEqualTo("NewYork");
			assertThat(found2.getVersion().get()).isEqualTo(
					referenceCount.get());
		} else {
			CompleteBean reference = entity2;
			Counter referenceCount = counter2;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isTrue();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found1.getPreferences().get(1)).isEqualTo("US");
			assertThat(found1.getPreferences().get(2)).isEqualTo("NewYork");
			assertThat(found1.getVersion().get()).isEqualTo(
					referenceCount.get());

			reference = entity1;
			referenceCount = counter1;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isTrue();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found2.getPreferences().get(1)).isEqualTo("FR");
			assertThat(found2.getPreferences().get(2)).isEqualTo("Paris");
			assertThat(found2.getPreferences().get(3)).isEqualTo("75014");
			assertThat(found2.getVersion().get()).isEqualTo(
					referenceCount.get());
		}
	}

	@Test
	public void should_return_entities_for_typed_query_with_simple_select()
			throws Exception {
		Counter counter1 = CounterBuilder.incr(15L);
		CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(counter1).buid();

		Counter counter2 = CounterBuilder.incr(17L);
		CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId()
				.name("John DOO").age(34L).addFriends("qux", "twix")
				.addFollowers("Isaac", "Lara").addPreference(1, "US")
				.addPreference(2, "NewYork").version(counter2).buid();

		em.persist(entity1);
		em.persist(entity2);

		String queryString = "SELECT id,name,friends FROM CompleteBean LIMIT 3";
		List<CompleteBean> actual = em.typedQuery(CompleteBean.class,
				queryString).get();

		assertThat(actual).hasSize(2);

		CompleteBean found1 = actual.get(0);
		CompleteBean found2 = actual.get(1);

		Factory factory1 = (Factory) found1;
		CQLEntityInterceptor<CompleteBean> interceptor1 = (CQLEntityInterceptor<CompleteBean>) factory1
				.getCallback(0);
		assertThat(interceptor1.getAlreadyLoaded()).hasSize(3);

		CompleteBean target1 = (CompleteBean) interceptor1.getTarget();

		assertThat(target1.getAge()).isNull();
		assertThat(target1.getFollowers()).isNull();
		assertThat(target1.getLabel()).isNull();
		assertThat(target1.getPreferences()).isNull();
		assertThat(target1.getWelcomeTweet()).isNull();

		Factory factory2 = (Factory) found1;
		CQLEntityInterceptor<CompleteBean> interceptor2 = (CQLEntityInterceptor<CompleteBean>) factory2
				.getCallback(0);
		assertThat(interceptor2.getAlreadyLoaded()).hasSize(3);

		CompleteBean target2 = (CompleteBean) interceptor2.getTarget();

		assertThat(target2.getAge()).isNull();
		assertThat(target2.getFollowers()).isNull();
		assertThat(target2.getLabel()).isNull();
		assertThat(target2.getPreferences()).isNull();
		assertThat(target2.getWelcomeTweet()).isNull();

		if (found1.getId().equals(entity1.getId())) {
			CompleteBean reference = entity1;
			Counter referenceCount = counter1;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isTrue();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getVersion().get()).isEqualTo(
					referenceCount.get());

			reference = entity2;
			referenceCount = counter2;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isTrue();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getVersion().get()).isEqualTo(
					referenceCount.get());
		} else {
			CompleteBean reference = entity2;
			Counter referenceCount = counter2;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isTrue();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getVersion().get()).isEqualTo(
					referenceCount.get());

			reference = entity1;
			referenceCount = counter1;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isTrue();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getVersion().get()).isEqualTo(
					referenceCount.get());
		}
	}

	@Test
	public void should_return_raw_entities_for_raw_typed_query_with_select_star()
			throws Exception {
		Counter counter1 = CounterBuilder.incr(15L);
		CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(counter1).buid();

		Counter counter2 = CounterBuilder.incr(17L);
		CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId()
				.name("John DOO").age(34L).addFriends("qux", "twix")
				.addFollowers("Isaac", "Lara").addPreference(1, "US")
				.addPreference(2, "NewYork").version(counter2).buid();

		em.persist(entity1);
		em.persist(entity2);

		String queryString = "SELECT * FROM CompleteBean LIMIT 3";
		List<CompleteBean> actual = em.rawTypedQuery(CompleteBean.class,
				queryString).get();

		assertThat(actual).hasSize(2);

		CompleteBean found1 = actual.get(0);
		CompleteBean found2 = actual.get(1);

		if (found1.getId().equals(entity1.getId())) {
			CompleteBean reference = entity1;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isFalse();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getAge()).isEqualTo(reference.getAge());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found1.getPreferences().get(1)).isEqualTo("FR");
			assertThat(found1.getPreferences().get(2)).isEqualTo("Paris");
			assertThat(found1.getPreferences().get(3)).isEqualTo("75014");
			assertThat(found1.getVersion()).isNull();

			reference = entity2;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isFalse();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getAge()).isEqualTo(reference.getAge());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found2.getPreferences().get(1)).isEqualTo("US");
			assertThat(found2.getPreferences().get(2)).isEqualTo("NewYork");
			assertThat(found2.getVersion()).isNull();
		} else {
			CompleteBean reference = entity2;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isFalse();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found1.getPreferences().get(1)).isEqualTo("US");
			assertThat(found1.getPreferences().get(2)).isEqualTo("NewYork");
			assertThat(found1.getVersion()).isNull();

			reference = entity1;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isFalse();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getFollowers()).containsAll(
					reference.getFollowers());
			assertThat(found2.getPreferences().get(1)).isEqualTo("FR");
			assertThat(found2.getPreferences().get(2)).isEqualTo("Paris");
			assertThat(found2.getPreferences().get(3)).isEqualTo("75014");
			assertThat(found2.getVersion()).isNull();
		}
	}

	@Test
	public void should_return_raw_entities_for_raw_typed_query_with_simple_select()
			throws Exception {
		Counter counter1 = CounterBuilder.incr(15L);
		CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(counter1).buid();

		Counter counter2 = CounterBuilder.incr(17L);
		CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId()
				.name("John DOO").age(34L).addFriends("qux", "twix")
				.addFollowers("Isaac", "Lara").addPreference(1, "US")
				.addPreference(2, "NewYork").version(counter2).buid();

		em.persist(entity1);
		em.persist(entity2);

		String queryString = "  SELECT id, name, friends   FROM CompleteBean LIMIT 3";
		List<CompleteBean> actual = em.rawTypedQuery(CompleteBean.class,
				queryString).get();

		assertThat(actual).hasSize(2);

		CompleteBean found1 = actual.get(0);
		CompleteBean found2 = actual.get(1);

		if (found1.getId().equals(entity1.getId())) {
			CompleteBean reference = entity1;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isFalse();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getVersion()).isNull();

			reference = entity2;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isFalse();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getVersion()).isNull();
		} else {
			CompleteBean reference = entity2;

			assertThat(Factory.class.isAssignableFrom(found1.getClass()))
					.isFalse();
			assertThat(found1.getId()).isEqualTo(reference.getId());
			assertThat(found1.getName()).isEqualTo(reference.getName());
			assertThat(found1.getFriends()).containsAll(reference.getFriends());
			assertThat(found1.getVersion()).isNull();

			reference = entity1;

			assertThat(Factory.class.isAssignableFrom(found2.getClass()))
					.isFalse();
			assertThat(found2.getId()).isEqualTo(reference.getId());
			assertThat(found2.getName()).isEqualTo(reference.getName());
			assertThat(found2.getFriends()).containsAll(reference.getFriends());
			assertThat(found2.getVersion()).isNull();
		}
	}

	@Test
	public void should_return_first_entity_for_typed_query_with_simple_select()
			throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(CounterBuilder.incr(15L)).buid();

		em.persist(entity);

		String queryString = "SELECT id,name,friends FROM CompleteBean LIMIT 3";
		CompleteBean actual = em.typedQuery(CompleteBean.class, queryString)
				.getFirst();

		Factory factory1 = (Factory) actual;
		CQLEntityInterceptor<CompleteBean> interceptor1 = (CQLEntityInterceptor<CompleteBean>) factory1
				.getCallback(0);
		assertThat(interceptor1.getAlreadyLoaded()).hasSize(3);

		CompleteBean target1 = (CompleteBean) interceptor1.getTarget();

		assertThat(target1.getAge()).isNull();
		assertThat(target1.getFollowers()).isNull();
		assertThat(target1.getLabel()).isNull();
		assertThat(target1.getPreferences()).isNull();
		assertThat(target1.getWelcomeTweet()).isNull();

		assertThat(Factory.class.isAssignableFrom(actual.getClass())).isTrue();
		assertThat(actual.getId()).isEqualTo(entity.getId());
		assertThat(actual.getName()).isEqualTo(entity.getName());
		assertThat(actual.getFriends()).containsAll(entity.getFriends());
		assertThat(actual.getVersion().get()).isEqualTo(
				entity.getVersion().get());

	}

	@Test
	public void should_return_first_raw_entity_for_raw_typed_query_with_simple_select()
			throws Exception {
		CompleteBean entity = CompleteBeanTestBuilder.builder().randomId()
				.name("DuyHai").age(35L).addFriends("foo", "bar")
				.addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014")
				.version(CounterBuilder.incr(15L)).buid();

		em.persist(entity);

		String queryString = "SELECT id,name,friends FROM CompleteBean LIMIT 3";
		CompleteBean actual = em.rawTypedQuery(CompleteBean.class, queryString)
				.getFirst();

		assertThat(Factory.class.isAssignableFrom(actual.getClass())).isFalse();

		assertThat(actual.getId()).isEqualTo(entity.getId());
		assertThat(actual.getName()).isEqualTo(entity.getName());
		assertThat(actual.getLabel()).isNull();
		assertThat(actual.getAge()).isNull();
		assertThat(actual.getFriends()).containsAll(entity.getFriends());
		assertThat(actual.getFollowers()).isNull();
		assertThat(actual.getPreferences()).isNull();
		assertThat(actual.getVersion()).isNull();
		assertThat(actual.getWelcomeTweet()).isNull();

	}
}
