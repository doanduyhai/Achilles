/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.types;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import info.archinnov.achilles.configuration.ConfigurationParameters;

public class ConfigMap extends LinkedHashMap<ConfigurationParameters, Object> {

    private static final long serialVersionUID = 1L;

    public static ConfigMap fromMap(Map<ConfigurationParameters, Object> source) {
        ConfigMap configMap = new ConfigMap();
        configMap.addAll(source);
        return configMap;
    }

    @SuppressWarnings("unchecked")
    public <T> T getTyped(ConfigurationParameters key) {
        T value = null;
        if (super.containsKey(key) && super.get(key) != null) {
            value = (T) super.get(key);
            return value;
        }
        return value;
    }

    public <T> T getTypedOr(ConfigurationParameters key, T defaultValue) {
        if (super.containsKey(key)) {
            return getTyped(key);
        } else {
            return defaultValue;
        }
    }

    public <T> T getTypedOr(ConfigurationParameters key, Supplier<T> defaultValue) {
        if (super.containsKey(key)) {
            return getTyped(key);
        } else {
            return defaultValue.get();
        }
    }

    private void addAll(Map<ConfigurationParameters, Object> source) {
        for (Map.Entry<ConfigurationParameters, Object> entry : source.entrySet()) {
            super.put(entry.getKey(), entry.getValue());
        }
    }
}
