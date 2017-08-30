/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

public class DefaultBeanFactory implements BeanFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBeanFactory.class);

    @Override
    public <T> T newInstance(Class<T> clazz) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Creating new instance of class %s", clazz.getCanonicalName()));
        }
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AchillesException(format("Cannot instantiate instance of class '%s'. Did you forget to declare a default constructor ?", clazz.getCanonicalName()));
        }
    }
}
