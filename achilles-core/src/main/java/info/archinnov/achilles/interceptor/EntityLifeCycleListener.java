package info.archinnov.achilles.interceptor;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.validation.Validator;

import java.util.List;
import java.util.Map;

public class EntityLifeCycleListener<CONTEXT extends PersistenceContext> {

	private EntityProxifier<CONTEXT> proxifier;
	private Map<Class<?>, EntityMeta> entityMetaMap;

	public EntityLifeCycleListener(EntityProxifier<CONTEXT> proxifier, Map<Class<?>, EntityMeta> entityMetaMap) {
		this.entityMetaMap = entityMetaMap;
		this.proxifier = proxifier;
	}

	public EntityLifeCycleListener(EntityProxifier<CONTEXT> proxifier) {
		this.proxifier = proxifier;
	}

	public void intercept(Object entity, Event event) {
		Class<?> baseClass = proxifier.deriveBaseClass(entity);
		EntityMeta entityMeta = entityMetaMap.get(baseClass);
		List<EventInterceptor<? extends Object>> eventInterceptors = entityMeta.getEventsInterceptor(event);
		if (eventInterceptors.size() > 0) {
			for (EventInterceptor eventInterceptor : eventInterceptors) {
				eventInterceptor.onEvent(entity);
			}
			Validator.validateNotNull(entity, "The entity class should not be null after interceptor, event:" + event);
			Validator.validateNotNull(entityMeta.getPrimaryKey(entity), "The primary key should not be null after interceptor, event:" + event);
		}

	}
}
