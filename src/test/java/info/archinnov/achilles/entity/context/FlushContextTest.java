package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.CounterDao.COUNTER_CF;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.CounterDao;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.dao.GenericEntityDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.context.FlushContext.BatchType;
import info.archinnov.achilles.entity.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * BatchContextTest
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings(
{
		"unchecked",
		"rawtypes"
})
@RunWith(MockitoJUnitRunner.class)
public class FlushContextTest
{
	@InjectMocks
	private FlushContext context;

	private Map<String, GenericEntityDao<?>> entityDaosMap = new HashMap<String, GenericEntityDao<?>>();

	private Map<String, GenericColumnFamilyDao<?, ?>> columnFamilyDaosMap = new HashMap<String, GenericColumnFamilyDao<?, ?>>();

	@Mock
	private CounterDao counterDao;

	@Mock
	private GenericEntityDao<Long> entityDao;

	@Mock
	private GenericColumnFamilyDao<Long, String> cfDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private ConsistencyContext consistencyContext;

	private Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>>();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(context, "type", BatchType.NONE);
		Whitebox.setInternalState(context, "consistencyContext", consistencyContext);
		Whitebox.setInternalState(context, "mutatorMap", mutatorMap);
		Whitebox.setInternalState(context, "entityDaosMap", entityDaosMap);
		Whitebox.setInternalState(context, "columnFamilyDaosMap", columnFamilyDaosMap);
		mutatorMap.clear();
		entityDaosMap.clear();
		columnFamilyDaosMap.clear();
	}

	@Test
	public void should_start_batch() throws Exception
	{
		context.startBatch();
		assertThat(context.type()).isEqualTo(BatchType.BATCH);
	}

	@Test
	public void should_flush() throws Exception
	{
		Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = new Pair(mutator, entityDao);
		mutatorMap.put("cf", pair);

		context.flush();

		verify(entityDao).executeMutator(mutator);
		verify(consistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_end_batch() throws Exception
	{
		Whitebox.setInternalState(context, "type", BatchType.BATCH);
		Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = new Pair(mutator, entityDao);
		mutatorMap.put("cf", pair);

		context.endBatch();

		verify(entityDao).executeMutator(mutator);
		verify(consistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_set_write_consistency_level() throws Exception
	{
		context.setWriteConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		verify(consistencyContext).setWriteConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_set_read_consistency_level() throws Exception
	{
		context.setReadConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		verify(consistencyContext).setReadConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_reinit_consistency_levels() throws Exception
	{
		Whitebox.setInternalState(context, "hasCustomConsistencyLevels", false);
		context.reinitConsistencyLevels();
		verify(consistencyContext).reinitConsistencyLevels();
	}

	@Test
	public void should_get_existing_entity_mutator() throws Exception
	{
		Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = new Pair(mutator, entityDao);
		mutatorMap.put("cf", pair);

		Mutator<Long> actual = context.getEntityMutator("cf");
		assertThat(actual).isSameAs(mutator);
	}

	@Test
	public void should_get_new_entity_mutator() throws Exception
	{
		entityDaosMap.put("cf", entityDao);
		when(entityDao.buildMutator()).thenReturn(mutator);

		Mutator<Long> actual = context.getEntityMutator("cf");
		assertThat(actual).isSameAs(mutator);
		assertThat((Mutator<Long>) mutatorMap.get("cf").left).isSameAs(mutator);
		assertThat((GenericEntityDao<Long>) mutatorMap.get("cf").right).isSameAs(entityDao);
	}

	@Test
	public void should_get_existing_cf_mutator() throws Exception
	{
		Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = new Pair(mutator, entityDao);
		mutatorMap.put("cf", pair);

		Mutator<Long> actual = context.getColumnFamilyMutator("cf");
		assertThat(actual).isSameAs(mutator);
	}

	@Test
	public void should_get_new_cf_mutator() throws Exception
	{
		columnFamilyDaosMap.put("cf", cfDao);
		when(cfDao.buildMutator()).thenReturn(mutator);

		Mutator<Long> actual = context.getColumnFamilyMutator("cf");
		assertThat(actual).isSameAs(mutator);
		assertThat((Mutator<Long>) mutatorMap.get("cf").left).isSameAs(mutator);
		assertThat((GenericColumnFamilyDao<Long, String>) mutatorMap.get("cf").right).isSameAs(
				cfDao);
	}

	@Test
	public void should_get_existing_counter_mutator() throws Exception
	{
		Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = new Pair(counterMutator, counterDao);
		mutatorMap.put(CounterDao.COUNTER_CF, pair);

		Mutator<Composite> actual = context.getCounterMutator();
		assertThat(actual).isSameAs(counterMutator);
	}

	@Test
	public void should_get_new_counter_mutator() throws Exception
	{
		when(counterDao.buildMutator()).thenReturn(counterMutator);

		Mutator<Composite> actual = context.getCounterMutator();

		assertThat(actual).isSameAs(counterMutator);
		assertThat((Mutator<Composite>) mutatorMap.get(COUNTER_CF).left).isSameAs(counterMutator);
		assertThat((CounterDao) mutatorMap.get(COUNTER_CF).right).isSameAs(counterDao);
	}
}
