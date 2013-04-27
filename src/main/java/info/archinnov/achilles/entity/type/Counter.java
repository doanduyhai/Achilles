package info.archinnov.achilles.entity.type;

import java.io.Serializable;

/**
 * Counter
 * 
 * @author DuyHai DOAN
 * 
 */
public interface Counter extends Serializable
{
	public Long get();

	public void incr();

	public void incr(Long increment);

	public void decr();

	public void decr(Long decrement);

}
