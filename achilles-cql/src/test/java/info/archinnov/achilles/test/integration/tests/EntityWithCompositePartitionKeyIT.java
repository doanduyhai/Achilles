package info.archinnov.achilles.test.integration.tests;

import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.manager.CQLEntityManager;
import info.archinnov.achilles.junit.AchillesInternalCQLResource;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.EmbeddedKey;
import net.sf.cglib.proxy.Factory;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class EntityWithCompositePartitionKeyIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST,
			EntityWithCompositePartitionKey.class.getSimpleName());

	private CQLEntityManager em = resource.getEm();

	private Session session = em.getNativeSession();

	@Test
	public void should_persist() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		em.persist(entity);

		Row row = session.execute("SELECT * FROM EntityWithCompositePartitionKey WHERE id=" + id + " AND type='type'")
				.one();

		assertThat(row).isNotNull();
		assertThat(row.getLong("id")).isEqualTo(id);
		assertThat(row.getString("type")).isEqualTo("type");
		assertThat(row.getString("value")).isEqualTo("value");
	}

	@Test
	public void should_find() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		em.persist(entity);

		EntityWithCompositePartitionKey found = em.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));

		assertThat(found).isNotNull();
		assertThat(found).isInstanceOf(Factory.class);
		assertThat(found.getId().getId()).isEqualTo(id);
		assertThat(found.getId().getType()).isEqualTo("type");
		assertThat(found.getValue()).isEqualTo("value");
	}

	@Test
	public void should_merge_and_get_reference() throws Exception {
		long id = RandomUtils.nextLong();
		EmbeddedKey compositeRowKey = new EmbeddedKey(id, "type");

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "clustered_value");

		em.merge(entity);

		EntityWithCompositePartitionKey found = em.getReference(EntityWithCompositePartitionKey.class, compositeRowKey);

		assertThat(found.getId()).isEqualTo(compositeRowKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_modifications() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = em.merge(entity);

		entity.setValue("value2");
		em.merge(entity);

		EntityWithCompositePartitionKey found = em.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found.getValue()).isEqualTo("value2");
	}

	@Test
	public void should_remove() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = em.merge(entity);

		em.remove(entity);

		EntityWithCompositePartitionKey found = em.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found).isNull();
	}

	@Test
	public void should_remove_by_id() throws Exception {
		long id = RandomUtils.nextLong();
		EmbeddedKey compositeRowKey = new EmbeddedKey(id, "type");

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "clustered_value");

		entity = em.merge(entity);

		em.removeById(EntityWithCompositePartitionKey.class, compositeRowKey);

		assertThat(em.find(EntityWithCompositePartitionKey.class, compositeRowKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {
		long id = RandomUtils.nextLong();

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = em.merge(entity);

		session.execute("UPDATE EntityWithCompositePartitionKey SET value='new_value' WHERE id=" + id
				+ " AND type='type'");

		em.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_value");
	}
}
