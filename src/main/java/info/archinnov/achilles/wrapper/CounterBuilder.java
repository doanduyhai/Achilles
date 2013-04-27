package info.archinnov.achilles.wrapper;

import info.archinnov.achilles.entity.type.Counter;

/**
 * CounterImpl
 * 
 * @author DuyHai DOAN
 * 
 */

public class CounterBuilder
{
	public static Counter incr()
	{
		return new CounterImpl(1L);
	}

	public static Counter incr(Long incr)
	{
		return new CounterImpl(incr);
	}

	public static Counter decr()
	{
		return new CounterImpl(-1L);
	}

	public static Counter decr(Long decr)
	{
		return new CounterImpl(-1L * decr);
	}

	public static class CounterImpl implements Counter
	{
		private static final long serialVersionUID = 1L;
		private final Long value;

		private CounterImpl(Long value) {
			this.value = value;
		}

		@Override
		public Long get()
		{
			return value;
		}

		@Override
		public void incr()
		{
			throw new UnsupportedOperationException("This method is not mean to be called");
		}

		@Override
		public void incr(Long increment)
		{
			throw new UnsupportedOperationException("This method is not mean to be called");
		}

		@Override
		public void decr()
		{
			throw new UnsupportedOperationException("This method is not mean to be called");
		}

		@Override
		public void decr(Long decrement)
		{
			throw new UnsupportedOperationException("This method is not mean to be called");
		}

	}
}
