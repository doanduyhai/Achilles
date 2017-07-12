/*
 * Copyright (C) 2012-2016 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.internals.factory;

import static java.lang.String.format;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.factory.BeanFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class DefaultBeanFactory implements BeanFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBeanFactory.class);

    @Override
    public <T> T newInstance(Constructor<T> constructor, Object[] args) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Creating new instance of class %s", constructor.getClass().getCanonicalName()));
        }
        try {
            return constructor.newInstance(args);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new AchillesException(format("Cannot instantiate instance of class '%s'. Did you forget to declare a default constructor ?",
                    constructor.getDeclaringClass().getCanonicalName()));
        }
    }
}
