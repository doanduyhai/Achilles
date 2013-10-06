package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.MessageWithSecondaryIndex;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.IndexEquality;

import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MessageWithSecondaryIndexIT {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "clustered");

	private CQLEntityManager em = resource.getEm();

	@Test
	public void should_query_with_index_conditions() throws Exception {
		insertEntities(6L);
		List<MessageWithSecondaryIndex> entities = em.sliceQuery(MessageWithSecondaryIndex.class)
				.conditions(Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "message_2")), false).get();

		assertThat(entities).hasSize(1);
		MessageWithSecondaryIndex message = entities.get(0);

		assertThat(message.getId()).isEqualTo(2L);

		em.persist(new MessageWithSecondaryIndex(5L, "message_2", 5));
		em.persist(new MessageWithSecondaryIndex(6L, "message_2", 6));
		em.persist(new MessageWithSecondaryIndex(7L, "message_2", 7));
		em.persist(new MessageWithSecondaryIndex(8L, "message_2", 8));

		entities = em
				.sliceQuery(MessageWithSecondaryIndex.class)
				.conditions(
						Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "message_2"),
								new IndexCondition("number", IndexEquality.GREATER, 5), new IndexCondition("number",
										IndexEquality.LESS_OR_EQUAL, 8)), true)// allow
																				// filtering
				.get();

		assertThat(entities).hasSize(3);

	}

	private void insertEntities(long count) {
		for (long i = 0; i < count; i++) {
			MessageWithSecondaryIndex message = new MessageWithSecondaryIndex(i, "message_" + i, (int) i);
			em.persist(message);
		}
	}
}
