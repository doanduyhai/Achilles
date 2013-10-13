package info.archinnov.achilles.test.integration.tests;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static info.archinnov.achilles.table.TableNameNormalizer.*;
import static info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.*;
import static org.fest.assertions.api.Assertions.*;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.manager.ThriftPersistenceManager;
import info.archinnov.achilles.junit.AchillesTestResource.Steps;
import info.archinnov.achilles.test.integration.AchillesInternalThriftResource;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey;
import info.archinnov.achilles.test.integration.entity.EntityWithCompositePartitionKey.EmbeddedKey;

import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.Factory;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Optional;

public class EntityWithCompositePartitionKeyIT {

	@Rule
	public AchillesInternalThriftResource resource = new AchillesInternalThriftResource(Steps.AFTER_TEST, TABLE_NAME);

	private ThriftPersistenceManager manager = resource.getPersistenceManager();

	private ThriftGenericEntityDao dao = resource.getEntityDao(normalizerAndValidateColumnFamilyName(TABLE_NAME),
			Composite.class);

	private byte[] START_EAGER = new byte[] { 0 };
	private byte[] END_EAGER = new byte[] { 20 };

	@Test
	public void should_persist() throws Exception {
		Long id = RandomUtils.nextLong();
		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.persist(entity);

		Composite rowKey = new Composite();
		rowKey.addComponent(id, LONG_SRZ);
		rowKey.addComponent("type", STRING_SRZ);

		Composite startCompositeForEagerFetch = new Composite();
		startCompositeForEagerFetch.addComponent(0, START_EAGER, ComponentEquality.EQUAL);

		Composite endCompositeForEagerFetch = new Composite();
		endCompositeForEagerFetch.addComponent(0, END_EAGER, ComponentEquality.GREATER_THAN_EQUAL);

		List<Pair<Composite, String>> columns = dao.findColumnsRange(rowKey, startCompositeForEagerFetch,
				endCompositeForEagerFetch, false, 20);

		assertThat(columns).hasSize(2);

		Pair<Composite, String> compositePartitionKey = columns.get(0);
		Pair<Composite, String> value = columns.get(1);

		assertThat(compositePartitionKey.left.get(1, STRING_SRZ)).isEqualTo("id");
		assertThat(compositePartitionKey.right).contains(id.toString()).contains("type");

		assertThat(value.left.get(1, STRING_SRZ)).isEqualTo("value");
		assertThat(value.right).isEqualTo("value");
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

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		manager.merge(entity);

		EntityWithCompositePartitionKey found = manager.getReference(EntityWithCompositePartitionKey.class,
				compositeRowKey);

		assertThat(found.getId()).isEqualTo(compositeRowKey);
		assertThat(found.getValue()).isEqualTo("value");
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

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.merge(entity);

		manager.removeById(EntityWithCompositePartitionKey.class, compositeRowKey);

		assertThat(manager.find(EntityWithCompositePartitionKey.class, compositeRowKey)).isNull();

	}

	@Test
	public void should_refresh() throws Exception {
		long id = RandomUtils.nextLong();

		EntityWithCompositePartitionKey entity = new EntityWithCompositePartitionKey(id, "type", "value");

		entity = manager.merge(entity);

		Composite rowKey = new Composite();
		rowKey.addComponent(id, LONG_SRZ);
		rowKey.addComponent("type", STRING_SRZ);

		Composite comp = new Composite();
		comp.setComponent(0, SIMPLE.flag(), BYTE_SRZ);
		comp.setComponent(1, "value", STRING_SRZ);
		comp.setComponent(2, "0", STRING_SRZ);

		Mutator<Composite> mutator = dao.buildMutator();
		dao.insertColumnBatch(rowKey, comp, "new_value", Optional.<Integer> absent(), Optional.<Long> absent(), mutator);
		dao.executeMutator(mutator);

		manager.refresh(entity);

		assertThat(entity.getValue()).isEqualTo("new_value");

	}
}
