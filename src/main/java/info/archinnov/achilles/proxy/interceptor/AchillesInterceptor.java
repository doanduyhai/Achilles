package info.archinnov.achilles.proxy.interceptor;


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
}
