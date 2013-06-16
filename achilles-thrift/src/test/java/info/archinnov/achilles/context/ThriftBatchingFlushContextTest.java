package info.archinnov.achilles.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.context.AchillesFlushContext.FlushType;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.type.Pair;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;

/**
 * ThriftBatchingFlushContextTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class ThriftBatchingFlushContextTest
{
	private ThriftBatchingFlushContext context;

	@Mock
	private ThriftCounterDao thriftCounterDao;

	@Mock
	private ThriftGenericEntityDao entityDao;

	@Mock
	private ThriftGenericWideRowDao cfDao;

	@Mock
	private Mutator<Long> mutator;

	@Mock
	private Mutator<Composite> counterMutator;

	@Mock
	private ThriftConsistencyContext thriftConsistencyContext;

	@Mock
	private ThriftDaoContext thriftDaoContext;

	private Map<String, Pair<Mutator<?>, ThriftAbstractDao>> mutatorMap = new HashMap<String, Pair<Mutator<?>, ThriftAbstractDao>>();

	private Boolean hasCustomConsistencyLevels = false;

	private Optional<Integer> ttlO = Optional.<Integer> absent();

	@Before
	public void setUp()
	{
		context = new ThriftBatchingFlushContext(thriftDaoContext, thriftConsistencyContext,
				new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>(),
				hasCustomConsistencyLevels, ttlO);

		Whitebox.setInternalState(context, "consistencyContext", thriftConsistencyContext);
		Whitebox.setInternalState(context, "mutatorMap", mutatorMap);
		Whitebox.setInternalState(context, "thriftDaoContext", thriftDaoContext);
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
		Pair<Mutator<?>, ThriftAbstractDao> pair = new Pair<Mutator<?>, ThriftAbstractDao>(mutator,
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

	@Test
	public void should_duplicate_without_ttl() throws Exception
	{
		context = new ThriftBatchingFlushContext(thriftDaoContext,
				thriftConsistencyContext,
				new HashMap<String, Pair<Mutator<Object>, ThriftAbstractDao>>(),
				true,
				Optional.fromNullable(10));
		ThriftBatchingFlushContext actual = context.duplicateWithoutTtl();

		assertThat(actual.ttlO.isPresent()).isFalse();
	}

}
