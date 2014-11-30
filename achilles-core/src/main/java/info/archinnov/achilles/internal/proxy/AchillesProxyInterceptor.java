package info.archinnov.achilles.internal.proxy;

import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public interface AchillesProxyInterceptor<T> {

    Object getTarget();

    void setTarget(T target);

    void setDirtyMap(Map<Method, DirtyChecker> dirtyMap);

    Map<Method, DirtyChecker> getDirtyMap();

    void setEntityOperations(EntityOperations context);

    Set<Method> getAlreadyLoaded();
}
