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

    static Counter initialValue(Long initialValue) {
        return new CounterImpl(0,initialValue);
    }
}
