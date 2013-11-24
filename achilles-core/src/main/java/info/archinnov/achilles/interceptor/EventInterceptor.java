package info.archinnov.achilles.interceptor;

import java.util.List;

public interface EventInterceptor<T> {

	public T onEvent(T entity);

	public List<Event> events();

}
