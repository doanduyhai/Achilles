package info.archinnov.achilles.type;

public class CounterBuilder {

    public static Counter incr() {
        return new CounterImpl(1L);
    }

    public static Counter incr(long incr) {
        return new CounterImpl(incr);
    }

    public static Counter decr() {
        return new CounterImpl(-1L);
    }

    public static Counter decr(long decr) {
        return new CounterImpl(-1L * decr);
    }

    public static class CounterImpl implements Counter {

        private long counterValue;

        private CounterImpl(long counterValue) {
            this.counterValue = counterValue;
        }

        @Override
        public Long get() {
            return counterValue;
        }

        @Override
        public void incr() {
            counterValue++;
        }

        @Override
        public void incr(long increment) {
            counterValue += increment;
        }

        @Override
        public void decr() {
            counterValue--;
        }

        @Override
        public void decr(long decrement) {
            counterValue -= decrement;
        }
    }
}
