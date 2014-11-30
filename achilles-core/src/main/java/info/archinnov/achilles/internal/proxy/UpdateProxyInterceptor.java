/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.proxy;

import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CounterLoader;
import info.archinnov.achilles.internal.persistence.operations.EntityLoader;
import info.archinnov.achilles.internal.persistence.operations.InternalCounterBuilder;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.internal.proxy.dirtycheck.SimpleDirtyChecker;
import info.archinnov.achilles.internal.proxy.wrapper.UpdateSetWrapper;
import info.archinnov.achilles.internal.proxy.wrapper.builder.*;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.Counter;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateProxyInterceptor<T> implements MethodInterceptor, ProxySerializable {

    private static final transient Logger log = LoggerFactory.getLogger(UpdateProxyInterceptor.class);

    private transient ReflectionInvoker invoker = ReflectionInvoker.Singleton.INSTANCE.get();

    private transient T target;
    private transient Object primaryKey;
    private transient Method idGetter;
    private transient Method idSetter;
    private transient Map<Method, PropertyMeta> getterMetas;
    private transient Map<Method, PropertyMeta> setterMetas;
    private transient Map<Method, DirtyChecker> dirtyMap;
    private transient EntityOperations context;

    public Object getTarget() {
        return this.target;
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        log.trace("Method {} called for entity of class {}", method.getName(), target.getClass().getCanonicalName());

        if (idGetter.equals(method)) {
            return primaryKey;
        } else if (idSetter.equals(method)) {
            throw new IllegalAccessException("Cannot change primary key value for existing entity ");
        }

        Object result = null;
        if (this.getterMetas.containsKey(method)) {
            result = interceptGetter(method);
        } else if (this.setterMetas.containsKey(method)) {
            interceptSetter(method, obj, args);
        } else {
            result = proxy.invoke(target, args);
        }
        return result;
    }

    private Object interceptGetter(Method method) throws Throwable {
        Object result;
        PropertyMeta propertyMeta = this.getterMetas.get(method);

        log.trace("Get value from field {} on real object", propertyMeta.getPropertyName());

        // Build proxy when necessary
        switch (propertyMeta.type()) {
            case LIST:
                log.trace("Build update list wrapper for property {} of entity of class {} ", propertyMeta.getPropertyName(),propertyMeta.getEntityClassName());
                result = UpdateListWrapperBuilder.builder().dirtyMap(dirtyMap).setter(propertyMeta.getSetter())
                        .propertyMeta(this.getPropertyMetaByProperty(method)).build();
                break;
            case SET:
                log.trace("Build update set wrapper for property {} of entity of class {} ", propertyMeta.getPropertyName(),propertyMeta.getEntityClassName());
                result = UpdateSetWrapperBuilder.builder().dirtyMap(dirtyMap).setter(propertyMeta.getSetter())
                        .propertyMeta(this.getPropertyMetaByProperty(method)).build();
                break;
            case MAP:
                log.trace("Build update map wrapper for property {} of entity of class {} ", propertyMeta.getPropertyName(),propertyMeta.getEntityClassName());
                result = UpdateMapWrapperBuilder.builder().dirtyMap(dirtyMap).setter(propertyMeta.getSetter())
                        .propertyMeta(this.getPropertyMetaByProperty(method)).build();
                break;
            default:
                throw new UnsupportedOperationException("Cannot call getter on non collection/map method. This proxy object is for update only, lazy-loading is not supported");
        }
        return result;
    }

    private void interceptSetter(Method method, Object obj, Object[] args) throws Throwable {
        PropertyMeta propertyMeta = this.setterMetas.get(method);

        DirtyChecker dirtyChecker = null;
        boolean removeField = false;
        if (args[0] == null) {
            removeField = true;
        }
        switch (propertyMeta.type()) {
            case SIMPLE:
                dirtyChecker = new SimpleDirtyChecker(propertyMeta);
                break;
            case SET:
                dirtyChecker = new DirtyChecker(propertyMeta);
                if (removeField)
                    dirtyChecker.removeAllElements();
                else
                    dirtyChecker.assignValue((Set) args[0]);
                break;
            case LIST:
                dirtyChecker = new DirtyChecker(propertyMeta);
                if (removeField)
                    dirtyChecker.removeAllElements();
                else
                    dirtyChecker.assignValue((List) args[0]);
                break;
            case MAP:
                dirtyChecker = new DirtyChecker(propertyMeta);
                if (removeField)
                    dirtyChecker.removeAllElements();
                else
                    dirtyChecker.assignValue((Map) args[0]);
                break;
            case COUNTER:
                throw new UnsupportedOperationException(
                        "Cannot set value directly to a Counter type. Please call the getter first to get handle on the wrapper");
            default:
                break;
        }

        log.trace("Flagging property {}", propertyMeta.getPropertyName());

        dirtyMap.put(method, dirtyChecker);
        Object value = null;
        if (args.length > 0) {
            value = args[0];
        }
        propertyMeta.forValues().setValueToField(target, value);
    }

    @Override
    public Object writeReplace() {
        return this.target;
    }

    public Map<Method, DirtyChecker> getDirtyMap() {
        return dirtyMap;
    }

    public Object getPrimaryKey() {
        return primaryKey;
    }

    public void setTarget(T target) {
        this.target = target;
    }

    void setPrimaryKey(Object key) {
        this.primaryKey = key;
    }

    void setIdGetter(Method idGetter) {
        this.idGetter = idGetter;
    }

    void setIdSetter(Method idSetter) {
        this.idSetter = idSetter;
    }

    void setGetterMetas(Map<Method, PropertyMeta> getterMetas) {
        this.getterMetas = getterMetas;
    }

    void setSetterMetas(Map<Method, PropertyMeta> setterMetas) {
        this.setterMetas = setterMetas;
    }

    void setDirtyMap(Map<Method, DirtyChecker> dirtyMap) {
        this.dirtyMap = dirtyMap;
    }

    public EntityOperations getEntityOperations() {
        return context;
    }

    public void setEntityOperations(EntityOperations context) {
        this.context = context;
    }

    private PropertyMeta getPropertyMetaByProperty(Method method) {
        return getterMetas.get(method);
    }
}
