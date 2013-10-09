package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.entity.manager.CQLPersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
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
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private CQLPersistenceManager manager = resource.getPersistenceManager();

	private Session session = manager.getNativeSession();

	@Test
	public void should_persist() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.persist(entity);

		Row row = session.execute("SELECT * FROM " + TABLE_NAME + " WHERE id=" + id + " AND type='type'").one();

		assertThat(row).isNotNull();
		assertThat(row.getLong("id")).isEqualTo(id);
		assertThat(row.getString("type")).isEqualTo("type");
		assertThat(row.getString("value")).isEqualTo("value");
	}

	@Test
	public void should_find() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.persist(entity);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
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

		manager.merge(entity);

		EntityWithCompositePartitionKey found = manager.getReference(EntityWithCompositePartitionKey.class,
				compositeRowKey);

		assertThat(found.getId()).isEqualTo(compositeRowKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_merge_modifications() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.merge(entity);

		entity.setValue("value2");
		manager.merge(entity);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found.getValue()).isEqualTo("value2");
	}

	@Test
	public void should_remove() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.merge(entity);

		manager.remove(entity);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found).isNull();
	}

	@Test
	public void should_remove_by_id() throws Exception {
		long id = RandomUtils.nextLong();
		EmbeddedKey compositeRowKey = new EmbeddedKey(id, "type");

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "clustered_value");

		entity = manager.merge(entity);

		manager.removeById(EntityWithCompositePartitionKey.class, compositeRowKey);

		assertThat(manager.find(EntityWithCompositePartitionKey.class, compositeRowKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {
		long id = RandomUtils.nextLong();

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.merge(entity);

		session.execute("UPDATE " + TABLE_NAME + " SET value='new_value' WHERE id=" + id + " AND type='type'");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_value");
	}
}
