package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageId.Type;
import info.archinnov.achilles.test.integration.entity.ClusteredEntity;
import info.archinnov.achilles.test.integration.entity.ClusteredMessageWithSecondaryIndex;
import info.archinnov.achilles.test.integration.entity.MessageWithSecondaryIndex;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.IndexEquality;

import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClusteredMessageWithSecondaryIndexIT {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "clustered");

	private CQLEntityManager em = resource.getEm();

	@Test
	public void should_query_with_clustered_key_and_index_conditions() throws Exception {
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(1L, Type.FILE), "desc", "first", 1));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(2L, Type.FILE), "desc", "second", 2));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(1L, Type.AUDIO), "desc", "first", 3));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(2L, Type.AUDIO), "desc", "second", 4));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(1L, Type.IMAGE), "desc", "first", 5));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(2L, Type.IMAGE), "desc", "second", 6));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(1L, Type.TEXT), "desc", "first", 7));
		em.persist(new ClusteredMessageWithSecondaryIndex(new ClusteredMessageId(2L, Type.TEXT), "desc", "second", 8));

		List<ClusteredMessageWithSecondaryIndex> entities = em.sliceQuery(ClusteredMessageWithSecondaryIndex.class)
				.partitionKey(1L).get();

		assertThat(entities).hasSize(4);

		entities = em.sliceQuery(ClusteredMessageWithSecondaryIndex.class).partitionKey(1L).fromClusterings(Type.FILE)
				.toClusterings(Type.IMAGE).get();

		assertThat(entities).hasSize(2);

		entities = em.sliceQuery(ClusteredMessageWithSecondaryIndex.class).partitionKey(1L).fromClusterings(Type.FILE)
				.toClusterings(Type.TEXT).get();

		assertThat(entities).hasSize(3); // FILE, IMAGE, TEXT

		entities = em.sliceQuery(ClusteredMessageWithSecondaryIndex.class).partitionKey(1L).fromClusterings(Type.FILE)
				.toClusterings(Type.TEXT)
				.conditions(Arrays.asList(new IndexCondition("number", IndexEquality.EQUAL, 7)), false).get();

		assertThat(entities).hasSize(1);

		entities = em.sliceQuery(ClusteredMessageWithSecondaryIndex.class).partitionKey(1L)
				.conditions(Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "first")), false).get();

		assertThat(entities).hasSize(4);

		entities = em
				.sliceQuery(ClusteredMessageWithSecondaryIndex.class)
				.partitionKey(2L)
				.conditions(
						Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "second"), new IndexCondition(
								"number", IndexEquality.GREATER, 5), new IndexCondition("number",
								IndexEquality.LESS_OR_EQUAL, 8)), true).get(); // allow filtering

		assertThat(entities).hasSize(2);

		entities = em.sliceQuery(ClusteredMessageWithSecondaryIndex.class)
				.conditions(Arrays.asList(new IndexCondition("number", IndexEquality.EQUAL, 7)), false).get();

		assertThat(entities).hasSize(1);

		entities = em
				.sliceQuery(ClusteredMessageWithSecondaryIndex.class)
				.conditions(
						Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "first"), new IndexCondition(
								"number", IndexEquality.GREATER, 5), new IndexCondition("number",
								IndexEquality.LESS_OR_EQUAL, 8)), true)// allow
																		// filtering
				.get();

		assertThat(entities).hasSize(1);
	}

}
