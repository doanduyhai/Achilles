package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.CompleteBean;
import info.archinnov.achilles.test.integration.entity.CompleteBeanTestBuilder;
import info.archinnov.achilles.test.integration.entity.EntityWithSecondaryIndex;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.IndexRelation;

import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SecondaryIndexIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
			CompleteBean.class.getSimpleName(), EntityWithSecondaryIndex.class.getSimpleName());

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_return_entities_for_indexed_query() throws Exception {
		Counter counter1 = CounterBuilder.incr(15L);
		CompleteBean entity1 = CompleteBeanTestBuilder.builder().randomId().name("DuyHai").age(35L)
				.addFriends("foo", "bar").addFollowers("George", "Paul").addPreference(1, "FR")
				.addPreference(2, "Paris").addPreference(3, "75014").version(counter1).buid();

		Counter counter2 = CounterBuilder.incr(17L);
		CompleteBean entity2 = CompleteBeanTestBuilder.builder().randomId().name("John DOO").age(34L)
				.addFriends("qux", "twix").addFollowers("Isaac", "Lara").addPreference(1, "US")
				.addPreference(2, "NewYork").version(counter2).buid();

		manager.persist(entity1);
		manager.persist(entity2);

		IndexCondition condition = new IndexCondition("name", IndexRelation.EQUAL, "John DOO");
		List<CompleteBean> actual = manager.indexedQuery(CompleteBean.class, condition).get();

		assertThat(actual).hasSize(1);

		CompleteBean found1 = actual.get(0);
		assertThat(found1).isNotNull();

	}

	@Test
	public void should_throw_clustered_exception_for_indexed_query() throws Exception {
		IndexCondition condition = new IndexCondition("name", IndexRelation.EQUAL, "John DOO");

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Index query is not supported for clustered entity. Please use slice query API with index condition");

		manager.indexedQuery(ClusteredEntity.class, condition).get();
	}

	@Test
	public void should_throw_empty_condition_exception_for_indexed_query() throws Exception {

		exception.expect(AchillesException.class);
		exception.expectMessage("Index condition should not be null");

		manager.indexedQuery(CompleteBean.class, null).get();
	}

	@Test
	public void should_throw_empty_column_name_for_indexed_query() throws Exception {
		IndexCondition condition = new IndexCondition(null, IndexRelation.EQUAL, "John DOO");

		exception.expect(AchillesException.class);
		exception.expectMessage("Column name for index condition '" + condition + "' should be provided");

		manager.indexedQuery(CompleteBean.class, condition).get();
	}

	@Test
	public void should_perform_slice_query_with_only_index_condition() throws Exception {
		long partitionKey = RandomUtils.nextLong();
		insertValues(partitionKey, 5);

		List<EntityWithSecondaryIndex> result = manager.sliceQuery(EntityWithSecondaryIndex.class)
				.indexCondition(new IndexCondition("label", IndexRelation.EQUAL, "label3")).get();

		assertThat(result.size()).isEqualTo(1);

		EntityWithSecondaryIndex found = result.get(0);

		assertThat(found.getId().getId()).isEqualTo(partitionKey);
		assertThat(found.getId().getRank()).isEqualTo(3);
		assertThat(found.getLabel()).isEqualTo("label3");
		assertThat(found.getNumber()).isEqualTo(3);

	}

	private void insertValues(long partitionKey, int size) {
		String labelPrefix = "label";
		for (int i = 1; i <= size; i++) {
			insertClusteredEntity(partitionKey, i, labelPrefix + i, i);
		}
	}

	private void insertClusteredEntity(Long partitionKey, Integer date, String label, Integer number) {
		EntityWithSecondaryIndex entity = new EntityWithSecondaryIndex(partitionKey, date, label, number);
		manager.persist(entity);
	}
}
