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

import java.lang.reflect.Field;

public class FieldAccessor {

    @SuppressWarnings("unchecked")
    public <T> T getValueFromField(Field field, Object instance) throws IllegalAccessException {
        T result = null;
        if (instance != null) {
            makeFieldAccessibleIfNeeded(field);
            result = (T) field.get(instance);
        }

        return result;

    }

    public void setValueToField(Field field, Object instance, Object value) throws IllegalAccessException {
        if (instance != null) {
            makeFieldAccessibleIfNeeded(field);
            field.set(instance, value);
        }
    }

    private void makeFieldAccessibleIfNeeded(Field field) {
        if (!field.isAccessible()) {
            synchronized (field) {
                field.setAccessible(true);
            }
        }
    }

    public static enum Singleton {
        INSTANCE;

        private final FieldAccessor instance = new FieldAccessor();

        public FieldAccessor get() {
            return instance;
        }
    }
}
