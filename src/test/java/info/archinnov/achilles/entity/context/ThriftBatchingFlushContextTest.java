package info.archinnov.achilles.entity.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.context.AchillesFlushContext.FlushType;
import info.archinnov.achilles.entity.type.Pair;

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
 * BatchingFlushContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftBatchingFlushContextTest
{
	@InjectMocks
	private ThriftBatchingFlushContext context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftGenericEntityDao<Long> entityDao;

	@Mock
	private ThriftGenericWideRowDao<Long, String> cfDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private ThriftConsistencyContext thriftConsistencyContext;

	@Mock
	private DaoContext daoContext;

	private Map<String, Pair<Mutator<?>, ThriftAbstractDao<?, ?>>> mutatorMap = new HashMap<String, Pair<Mutator<?>, ThriftAbstractDao<?, ?>>>();

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(context, "consistencyContext", thriftConsistencyContext);
		Whitebox.setInternalState(context, "mutatorMap", mutatorMap);
		Whitebox.setInternalState(context, "daoContext", daoContext);
		mutatorMap.clear();
	}

	@Test
	public void should_start_batch() throws Exception
	{
		context.startBatch();
		verify(thriftConsistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_do_nothing_when_flush_called() throws Exception
	{
		context.flush();
		verifyZeroInteractions(entityDao, thriftConsistencyContext);
	}

	@Test
	public void should_end_batch() throws Exception
	{
		Pair<Mutator<?>, ThriftAbstractDao<?, ?>> pair = new Pair<Mutator<?>, ThriftAbstractDao<?, ?>>(mutator,
				entityDao);
		mutatorMap.put("cf", pair);

		context.endBatch();

		verify(entityDao).executeMutator(mutator);
		verify(thriftConsistencyContext).reinitConsistencyLevels();
		assertThat(mutatorMap).isEmpty();
	}

	@Test
	public void should_get_type() throws Exception
	{
		assertThat(context.type()).isSameAs(FlushType.BATCH);
	}

}
