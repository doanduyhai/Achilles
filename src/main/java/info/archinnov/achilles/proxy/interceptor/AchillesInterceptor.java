package info.archinnov.achilles.proxy.interceptor;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * AchillesInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public interface AchillesInterceptor<ID>
{
	public Object getTarget();

	public Object getKey();

	public Mutator<ID> getMutator();

	public Mutator<ID> getEntityMutator(String columnFamilyName);

	public Mutator<ID> getColumnFamilyMutator(String columnFamilyName);

	public Mutator<Composite> getCounterMutator();
}
