package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.TABLE_NAME;
import static org.fest.assertions.api.Assertions.assertThat;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import info.archinnov.achilles.entity.manager.PersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalCQLResource;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.EmbeddedKey;
import net.sf.cglib.proxy.Factory;

public class EntityWithCompositePartitionKeyIT {

	@Rule
	public AchillesInternalCQLResource resource = new AchillesInternalCQLResource(Steps.AFTER_TEST, TABLE_NAME);

	private PersistenceManager manager = resource.getPersistenceManager();

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
	public void should_persist_and_get_proxy() throws Exception {
		long id = RandomUtils.nextLong();
		EmbeddedKey compositeRowKey = new EmbeddedKey(id, "type");

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "clustered_value");

		manager.persist(entity);

		EntityWithCompositePartitionKey found = manager.getProxy(EntityWithCompositePartitionKey.class,
                                                                 compositeRowKey);

		assertThat(found.getId()).isEqualTo(compositeRowKey);
		assertThat(found.getValue()).isEqualTo("clustered_value");
	}

	@Test
	public void should_update_modifications() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.persist(entity);

		entity.setValue("value2");
		manager.update(entity);

		EntityWithCompositePartitionKey found = manager.find(EntityWithCompositePartitionKey.class, new EmbeddedKey(id,
				"type"));
		assertThat(found.getValue()).isEqualTo("value2");
	}

	@Test
	public void should_remove() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.persist(entity);

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

		manager.persist(entity);

		manager.removeById(EntityWithCompositePartitionKey.class, compositeRowKey);

		assertThat(manager.find(EntityWithCompositePartitionKey.class, compositeRowKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {
		long id = RandomUtils.nextLong();

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.persist(entity);

		session.execute("UPDATE " + TABLE_NAME + " SET value='new_value' WHERE id=" + id + " AND type='type'");

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_value");
	}
}
