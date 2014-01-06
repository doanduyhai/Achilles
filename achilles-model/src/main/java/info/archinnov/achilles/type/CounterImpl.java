package info.archinnov.achilles.type;

public class CounterImpl implements Counter {

    private Long initialValue;

    private transient long delta;

    protected CounterImpl(long delta) {
        this.delta = delta;
    }

    protected CounterImpl(long delta,Long initialValue) {
        this.delta = delta;
        this.initialValue = initialValue;
    }

    @Override
    public Long get() {
        if (initialValue != null)
            return initialValue + delta;
        else
            return null;
    }

    @Override
    public void incr() {
        delta++;
    }

    @Override
    public void incr(long increment) {
        delta += increment;
    }

    @Override
    public void decr() {
        delta--;
    }

    @Override
    public void decr(long decrement) {
        delta -= decrement;
    }

    /**
     * <strong>Internal method, not to be used by clients</strong>
     *
     * @return
     *      delta between the initial counter value and the <strong>current</strong> counter value
     */
    public long getInternalCounterDelta() {
        return delta;
    }
}