package info.archinnov.achilles.internal.persistence.operations;

import info.archinnov.achilles.type.Counter;

/**
 * <strong>Class internal to Achilles, DO NOT USE</strong>
 */
public class InternalCounterBuilder {
	public static Counter incr() {
		return new InternalCounterImpl(1L);
	}

	public static Counter incr(long incr) {
		return new InternalCounterImpl(incr);
	}

	public static Counter decr() {
		return new InternalCounterImpl(-1L);
	}

	public static Counter decr(long decr) {
		return new InternalCounterImpl(-1L * decr);
	}

	public static Counter initialValue(Long initialValue) {
		return new InternalCounterImpl(null, initialValue);
	}
}
