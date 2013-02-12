package info.archinnov.achilles.proxy.interceptor;

import me.prettyprint.hector.api.mutation.Mutator;

/**
 * AchillesInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesInterceptor
{
	public Object getTarget();

	public Object getKey();

	public Mutator<?> getMutator();
}
