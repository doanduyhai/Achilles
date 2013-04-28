package info.archinnov.achilles.wrapper;

import static info.archinnov.achilles.entity.type.ConsistencyLevel.EACH_QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.wrapper.CounterBuilder.CounterImpl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * CounterBuilderTest
 * 
 * @author DuyHai DOAN
 * 
 */
public class CounterBuilderTest
{

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void should_incr() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		assertThat(counter.get()).isEqualTo(1L);
	}

	@Test
	public void should_incr_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) CounterBuilder.incr(EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(1L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_incr_n() throws Exception
	{
		Counter counter = CounterBuilder.incr(10L);
		assertThat(counter.get()).isEqualTo(10L);
	}

	@Test
	public void should_incr_n_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) CounterBuilder.incr(10L, EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(10L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_decr() throws Exception
	{
		Counter counter = CounterBuilder.decr();
		assertThat(counter.get()).isEqualTo(-1L);
	}

	@Test
	public void should_decr_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) CounterBuilder.decr(EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(-1L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_decr_n() throws Exception
	{
		Counter counter = CounterBuilder.decr(10L);
		assertThat(counter.get()).isEqualTo(-10L);
	}

	@Test
	public void should_decr_n_with_consistency() throws Exception
	{
		CounterImpl counter = (CounterImpl) CounterBuilder.decr(10L, EACH_QUORUM);
		assertThat(counter.get()).isEqualTo(-10L);
		assertThat(counter.getWriteLevel()).isEqualTo(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_get_with_consistency() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.get(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_incr() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr();
	}

	@Test
	public void should_exception_when_calling_incr_n() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr(10L);
	}

	@Test
	public void should_exception_when_calling_decr() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr();
	}

	@Test
	public void should_exception_when_calling_decr_n() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr(10L);
	}

	@Test
	public void should_exception_when_calling_incr_with_consistency() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_incr_n_with_consistency() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.incr(10L, EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_decr_with_consistency() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr(EACH_QUORUM);
	}

	@Test
	public void should_exception_when_calling_decr_n_with_consistency() throws Exception
	{
		Counter counter = CounterBuilder.incr();
		exception.expect(UnsupportedOperationException.class);
		exception.expectMessage("This method is not mean to be called");
		counter.decr(10L, EACH_QUORUM);
	}
}
