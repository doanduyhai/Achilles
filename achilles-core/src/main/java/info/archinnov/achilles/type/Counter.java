package info.archinnov.achilles.type;

/**
 * Counter
 * 
 * @author DuyHai DOAN
 * 
 */
public interface Counter
{
    public Long get();

    public Long get(ConsistencyLevel readLevel);

    public void incr();

    public void incr(ConsistencyLevel writeLevel);

    public void incr(Long increment);

    public void incr(Long increment, ConsistencyLevel writeLevel);

    public void decr();

    public void decr(ConsistencyLevel writeLevel);

    public void decr(Long decrement);

    public void decr(Long decrement, ConsistencyLevel writeLevel);

}
