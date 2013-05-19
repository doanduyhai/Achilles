package info.archinnov.achilles.entity.context;

import static info.archinnov.achilles.dao.ThriftCounterDao.COUNTER_CF;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.context.AchillesFlushContext.FlushType;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import info.archinnov.achilles.entity.type.Pair;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
@RunWith(MockitoJUnitRunner.class)
public class ThriftImmediateFlushContextTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@InjectMocks
	private ThriftImmediateFlushContext context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao cfDao;

	@Mock
	private Mutator<Object> mutator;

	@Mock
	private Mutator<Object> counterMutator;

	@Mock
	private ThriftConsistencyContext thriftConsistencyContext;

	@Mock
	private DaoContext daoContext;

	private Map<String, Pair<Mutator<?>, ThriftAbstractDao>> mutatorMap = new HashMap<String, Pair<Mutator<?>, ThriftAbstractDao>>();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(context, "consistencyContext", thriftConsistencyContext);
		Whitebox.setInternalState(context, "mutatorMap", mutatorMap);
		Whitebox.setInternalState(context, "daoContext", daoContext);
		mutatorMap.clear();
	}

	@Test
	public void should_exception_when_start_batch() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot start a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
		context.startBatch();
	}

	@Test
	public void should_flush() throws Exception
	{
		Pair<Mutator<?>, ThriftAbstractDao> pair = new Pair<Mutator<?>, ThriftAbstractDao>(mutator,
				entityDao);
		mutatorMap.put("cf", pair);

		context.flush();

		verify(entityDao).executeMutator(mutator);
		verify(thriftConsistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_exception_when_end_batch() throws Exception
	{
		exception.expect(UnsupportedOperationException.class);
		exception
				.expectMessage("Cannot end a batch with a normal EntityManager. Please create a BatchingEntityManager instead");
		context.endBatch();

	}

	@Test
	public void should_set_write_consistency_level() throws Exception
	{
		context.setWriteConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		verify(thriftConsistencyContext).setWriteConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_set_read_consistency_level() throws Exception
	{
		context.setReadConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		verify(thriftConsistencyContext).setReadConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_reinit_consistency_levels() throws Exception
	{
		Whitebox.setInternalState(context, "hasCustomConsistencyLevels", false);
		context.reinitConsistencyLevels();
		verify(thriftConsistencyContext).reinitConsistencyLevels();
	}

	@Test
	public void should_get_existing_entity_mutator() throws Exception
	{
		Pair<Mutator<?>, ThriftAbstractDao> pair = new Pair<Mutator<?>, ThriftAbstractDao>(mutator,
				entityDao);
		mutatorMap.put("cf", pair);

		Mutator<Object> actual = context.getEntityMutator("cf");
		assertThat(actual).isSameAs(mutator);
	}

	@Test
	public void should_get_new_entity_mutator() throws Exception
	{
		when((ThriftGenericEntityDao) daoContext.findEntityDao("cf")).thenReturn(entityDao);
		when(entityDao.buildMutator()).thenReturn(mutator);

		Mutator<Object> actual = context.getEntityMutator("cf");
		assertThat(actual).isSameAs(mutator);
		assertThat((Mutator<Object>) mutatorMap.get("cf").left).isSameAs(mutator);
		assertThat(mutatorMap.get("cf").right).isSameAs(entityDao);
	}

	@Test
	public void should_get_existing_cf_mutator() throws Exception
	{
		Pair<Mutator<?>, ThriftAbstractDao> pair = new Pair<Mutator<?>, ThriftAbstractDao>(mutator,
				entityDao);
		mutatorMap.put("cf", pair);

		Mutator<Object> actual = context.getWideRowMutator("cf");
		assertThat(actual).isSameAs(mutator);
	}

	@Test
	public void should_get_new_cf_mutator() throws Exception
	{
		when((ThriftGenericWideRowDao) daoContext.findWideRowDao("cf")).thenReturn(cfDao);
		when(cfDao.buildMutator()).thenReturn(mutator);

		Mutator<Object> actual = context.getWideRowMutator("cf");
		assertThat(actual).isSameAs(mutator);
		assertThat((Mutator<Object>) mutatorMap.get("cf").left).isSameAs(mutator);
		assertThat(mutatorMap.get("cf").right).isSameAs(cfDao);
	}

	@Test
	public void should_get_existing_counter_mutator() throws Exception
	{
		Pair<Mutator<?>, ThriftAbstractDao> pair = new Pair<Mutator<?>, ThriftAbstractDao>(
				counterMutator, thriftCounterDao);
		mutatorMap.put(ThriftCounterDao.COUNTER_CF, pair);

		Mutator<Object> actual = context.getCounterMutator();
		assertThat(actual).isSameAs(counterMutator);
	}

	@Test
	public void should_get_new_counter_mutator() throws Exception
	{
		when(daoContext.getCounterDao()).thenReturn(thriftCounterDao);
		when(thriftCounterDao.buildMutator()).thenReturn(counterMutator);

		Mutator<Object> actual = context.getCounterMutator();

		assertThat(actual).isSameAs(counterMutator);
		assertThat((Mutator<Object>) mutatorMap.get(COUNTER_CF).left).isSameAs(counterMutator);
		assertThat(mutatorMap.get(COUNTER_CF).right).isSameAs(thriftCounterDao);
	}

	@Test
	public void should_get_type() throws Exception
	{
		assertThat(context.type()).isSameAs(FlushType.IMMEDIATE);
	}
}
