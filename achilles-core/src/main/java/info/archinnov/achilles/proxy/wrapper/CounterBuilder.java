package info.archinnov.achilles.proxy.wrapper;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;

/**
 * CounterBuilder
 * 
 * @author DuyHai DOAN
 * 
 */

public class CounterBuilder {
    public static Counter incr() {
        return new CounterImpl(1L);
    }

    public static Counter incr(Long incr) {
        return new CounterImpl(incr);
    }

    public static Counter decr() {
        return new CounterImpl(-1L);
    }

    public static Counter decr(Long decr) {
        return new CounterImpl(-1L * decr);
    }

    public static class CounterImpl implements Counter {

        private final Long value;

        private CounterImpl(Long value) {
            this.value = value;
        }

        @Override
        public Long get() {
            return value;
        }

        @Override
        public Long get(ConsistencyLevel readLevel) {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void incr() {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void incr(Long increment) {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void decr() {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void decr(Long decrement) {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void incr(ConsistencyLevel writeLevel) {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void incr(Long increment, ConsistencyLevel writeLevel) {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void decr(ConsistencyLevel writeLevel) {
            throw new UnsupportedOperationException("This method is not mean to be called");
        }

        @Override
        public void decr(Long decrement, ConsistencyLevel writeLevel) {
            throw new UnsupportedOperationException("This method is not mean to be called");

        }
    }
}
