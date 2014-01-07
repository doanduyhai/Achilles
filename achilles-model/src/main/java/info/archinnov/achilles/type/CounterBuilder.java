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

    /**
     *  <p>Create a counter with initial value</p>
     *  <br/><br/><br/>
     *  <p><strong>WARNING!!! At runtime, Achilles only persist delta values introduced by calls to incr(), decr(), incr(long val), decr(long val) and not the initial value itself !!!</strong></p>
     *  <br/><br/><br/>
     *  <p><strong>One common usage is to set initialValue to 0</strong></p>
     *
     * @param initialValue
     * @return
     *      Counter with initial value
     */
    public static Counter initialValue(Long initialValue) {
        return new CounterImpl(0,initialValue);
    }
}
