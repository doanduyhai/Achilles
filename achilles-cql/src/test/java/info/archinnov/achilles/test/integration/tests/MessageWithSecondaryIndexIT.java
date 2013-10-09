package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.MessageWithSecondaryIndex;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.IndexEquality;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MessageWithSecondaryIndexIT {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, "clustered");

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	@Test
	public void should_query_with_index_conditions() throws Exception {
		insertEntities(6L);
		List<MessageWithSecondaryIndex> entities = manager.sliceQuery(MessageWithSecondaryIndex.class)
				.indexedConditions(Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "message_2")), false).get();

		assertThat(entities).hasSize(1);
		MessageWithSecondaryIndex message = entities.get(0);

		assertThat(message.getId().getId()).isEqualTo(2L);
		assertThat(message.getId().getDate()).isInstanceOf(UUID.class);

        UUID date= UUIDGen.getTimeUUID();

		manager.persist(new MessageWithSecondaryIndex(5L, date, "message_2", 5));
		manager.persist(new MessageWithSecondaryIndex(6L, date, "message_2", 6));
		manager.persist(new MessageWithSecondaryIndex(7L, date, "message_2", 7));
		manager.persist(new MessageWithSecondaryIndex(8L, date, "message_2", 8));

		entities = manager
				.sliceQuery(MessageWithSecondaryIndex.class)
				.indexedConditions(
						Arrays.asList(new IndexCondition("label", IndexEquality.EQUAL, "message_2"),
								new IndexCondition("number", IndexEquality.GREATER, 5), new IndexCondition("number",
										IndexEquality.LESS_OR_EQUAL, 8)), true)// allow
																				// filtering
				.get();

		assertThat(entities).hasSize(3);

	}

	private void insertEntities(long count) {
        UUID date= UUIDGen.getTimeUUID();
		for (long i = 0; i < count; i++) {
			MessageWithSecondaryIndex message = new MessageWithSecondaryIndex(i, date, "message_" + i, (int) i);
			manager.persist(message);
		}
	}
}
