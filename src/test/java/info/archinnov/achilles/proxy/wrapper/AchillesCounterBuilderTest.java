package info.archinnov.achilles.proxy.wrapper;

import static info.archinnov.achilles.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.proxy.wrapper.AchillesCounterBuilder.CounterImpl;
import info.archinnov.achilles.type.Counter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * AchillesCounterBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class AchillesCounterBuilderTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_incr() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		assertThat(counter.get()).isEqualTo(1L);
	}

	@Test
	public void should_incr_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) AchillesCounterBuilder.incr(EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(1L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_incr_n() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);
	}

	@Test
	public void should_incr_n_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) AchillesCounterBuilder.incr(10L, EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(10L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_decr() throws Exception
	{
		Counter counter = AchillesCounterBuilder.decr();
		assertThat(counter.get()).isEqualTo(-1L);
	}

	@Test
	public void should_decr_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) AchillesCounterBuilder.decr(EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(-1L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_decr_n() throws Exception
	{
		Counter counter = AchillesCounterBuilder.decr(10L);
		assertThat(counter.get()).isEqualTo(-10L);
	}

	@Test
	public void should_decr_n_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) AchillesCounterBuilder.decr(10L, EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(-10L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_get_with_consistency() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.get(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_incr() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr();
	}

	@Test
	public void should_exception_when_calling_incr_n() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr(10L);
	}

	@Test
	public void should_exception_when_calling_decr() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr();
	}

	@Test
	public void should_exception_when_calling_decr_n() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr(10L);
	}

	@Test
	public void should_exception_when_calling_incr_with_consistency() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_incr_n_with_consistency() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr(10L, EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_decr_with_consistency() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_decr_n_with_consistency() throws Exception
	{
		Counter counter = AchillesCounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr(10L, EACH_QUORUM);
	}
}
