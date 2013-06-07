package info.archinnov.achilles.context.execution;

/**
 * ThriftSafeExecutionContext
 * 
 * @author DuyHai DOAN
 * 
 */
public interface SafeExecutionContext<T>
{
	public T execute();
}
