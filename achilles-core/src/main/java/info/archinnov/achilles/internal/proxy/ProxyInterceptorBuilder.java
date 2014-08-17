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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.internal.context.facade.EntityOperations;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.internal.validation.Validator;

public class ProxyInterceptorBuilder<T> {
    private static final Logger log = LoggerFactory.getLogger(ProxyInterceptorBuilder.class);

    private T target;
    private Set<Method> alreadyLoaded = new HashSet<>();
    private EntityOperations context;

    public static <T> ProxyInterceptorBuilder<T> builder(EntityOperations context, T entity) {
        return new ProxyInterceptorBuilder<>(context, entity);
    }

    public ProxyInterceptorBuilder(EntityOperations context, T entity) {
        Validator.validateNotNull(context, "PersistenceContext for interceptor should not be null");
        Validator.validateNotNull(entity, "Target entity for interceptor should not be null");
        this.context = context;
        this.target = entity;
    }

    public ProxyInterceptor<T> build() {
        log.debug("Build interceptor for entity of class {}", context.getEntityMeta().getClassName());

        ProxyInterceptor<T> interceptor = new ProxyInterceptor<T>();

        EntityMeta entityMeta = context.getEntityMeta();

        String className = context.getEntityClass().getCanonicalName();
        Validator.validateNotNull(target, "Target object for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getGetterMetas(),
                "Getters metadata for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getSetterMetas(),
                "Setters metadata for interceptor of '%s' should not be null", className);
        Validator.validateNotNull(entityMeta.getIdMeta(), "Id metadata for '%s' should not be null", className);

        interceptor.setTarget(target);
        interceptor.setEntityOperations(context);
        interceptor.setGetterMetas(entityMeta.getGetterMetas());
        interceptor.setSetterMetas(entityMeta.getSetterMetas());
        interceptor.setIdGetter(entityMeta.getIdMeta().getGetter());
        interceptor.setIdSetter(entityMeta.getIdMeta().getSetter());
        interceptor.setDirtyMap(new HashMap<Method, DirtyChecker>());
        interceptor.setPrimaryKey(context.getPrimaryKey());
        interceptor.setAlreadyLoaded(alreadyLoaded);
        return interceptor;
    }

    public ProxyInterceptorBuilder<T> alreadyLoaded(Set<Method> alreadyLoaded) {
        this.alreadyLoaded = alreadyLoaded;
        return this;
    }
}
