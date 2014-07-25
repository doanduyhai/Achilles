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

package info.archinnov.achilles.internal.reflection;

import info.archinnov.achilles.exception.AchillesException;

public class ObjectInstantiator {

    public <T> T instantiate(Class<T> entityClass) {
        try {
            return entityClass.newInstance();
        } catch (InstantiationException e) {
            throw new AchillesException("Cannot instantiate class of type " + entityClass.getCanonicalName()+", did you forget to declare a default constructor ?",e);
        } catch (IllegalAccessException e) {
            throw new AchillesException("Cannot instantiate class of type " + entityClass.getCanonicalName()+", did you forget to declare a default constructor ?",e);
        }
    }

}
