package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.AchillesConfigurableConsistencyLevelPolicy;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.entity.type.ConsistencyLevel;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

/**
 * CounterWrapperTest
 * 
 * @author DuyHai DOAN
 * 
 */

@RunWith(MockitoJUnitRunner.class)
public class CounterWrapperTest
{
	@InjectMocks
	private CounterWrapper<Long> wrapper;

	private Long key = RandomUtils.nextLong();

	@Mock
	private Composite columnName;

	@Mock
	private AbstractDao<Long, Long> counterDao;

	@Mock
	private AchillesConfigurableConsistencyLevelPolicy policy;

	private ConsistencyLevel readLevel = EACH_QUORUM;
	private ConsistencyLevel writeLevel = LOCAL_QUORUM;

	@Before
	public void setUp()
	{
		Whitebox.setInternalState(wrapper, "key", key);
		wrapper.setColumnName(columnName);
		wrapper.setCounterDao(counterDao);
		wrapper.setPolicy(policy);
		wrapper.setReadLevel(readLevel);
		wrapper.setWriteLevel(writeLevel);
	}

	@Test
	public void should_get_counter() throws Exception
	{
		when(counterDao.getCounterValue(key, columnName)).thenReturn(10L);
		Long value = wrapper.get();

		assertThat(value).isEqualTo(10L);

		verify(policy).setCurrentReadLevel(readLevel);
		verify(policy).removeCurrentReadLevel();
	}

	@Test
	public void should_get_counter_with_existing_consistency_level() throws Exception
	{
		when(policy.getCurrentReadLevel()).thenReturn(QUORUM);
		when(counterDao.getCounterValue(key, columnName)).thenReturn(10L);
		Long value = wrapper.get();

		assertThat(value).isEqualTo(10L);

		verify(policy, never()).setCurrentReadLevel(readLevel);
		verify(policy, never()).removeCurrentReadLevel();
	}

	@Test
	public void should_incr() throws Exception
	{
		wrapper.incr();

		verify(counterDao).incrementCounter(key, columnName, 1L);
		verify(policy).setCurrentWriteLevel(writeLevel);
		verify(policy).removeCurrentWriteLevel();
	}

	@Test
	public void should_incr_with_existing_consistency_level() throws Exception
	{
		when(policy.getCurrentWriteLevel()).thenReturn(THREE);
		wrapper.incr();

		verify(counterDao).incrementCounter(key, columnName, 1L);
		verify(policy, never()).setCurrentWriteLevel(writeLevel);
		verify(policy, never()).removeCurrentWriteLevel();
	}

	@Test
	public void should_incr_with_value() throws Exception
	{
		wrapper.incr(10L);

		verify(counterDao).incrementCounter(key, columnName, 10L);
		verify(policy).setCurrentWriteLevel(writeLevel);
		verify(policy).removeCurrentWriteLevel();
	}

	@Test
	public void should_decr() throws Exception
	{
		wrapper.decr();

		verify(counterDao).decrementCounter(key, columnName, 1L);
		verify(policy).setCurrentWriteLevel(writeLevel);
		verify(policy).removeCurrentWriteLevel();
	}

	@Test
	public void should_decr_with_value() throws Exception
	{
		wrapper.decr(10L);

		verify(counterDao).decrementCounter(key, columnName, 10L);
		verify(policy).setCurrentWriteLevel(writeLevel);
		verify(policy).removeCurrentWriteLevel();
	}
}
