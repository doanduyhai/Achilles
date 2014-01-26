package info.archinnov.achilles.internal.persistence.operations;

import info.archinnov.achilles.type.Counter;

/**
 * <strong>Class internal to Achilles, DO NOT USE</strong>
 */
public class InternalCounterImpl implements Counter {

	private Long initialValue;

	private transient Long delta;

	protected InternalCounterImpl(long delta) {
		this.delta = delta;
	}

	protected InternalCounterImpl(Long delta, Long initialValue) {
		this.delta = delta;
		this.initialValue = initialValue;
	}

	@Override
	public Long get() {
		Long value;
		if (initialValue != null) {
			if (delta != null)
				value = initialValue + delta;
			else
				value = initialValue;
		} else {
			value = delta;
		}
		return value;
	}

	@Override
	public void incr() {
		delta = delta != null ? delta : 0L;
		delta++;
	}

	@Override
	public void incr(long increment) {
		delta = delta != null ? delta : 0L;
		delta += increment;
	}

	@Override
	public void decr() {
		delta = delta != null ? delta : 0L;
		delta--;
	}

	@Override
	public void decr(long decrement) {
		delta = delta != null ? delta : 0L;
		delta -= decrement;
	}

	/**
	 * <strong>Internal method, not to be used by clients !!!</strong>
	 * 
	 * @return delta between the initial counter value and the
	 *         <strong>current</strong> counter value
	 */
	public Long getInternalCounterDelta() {
		return delta;
	}
}