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

package info.archinnov.achilles.type;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class. A TypedMap is just a Linked HashMap&lt;String,Object&gt; with 2 extra methods:
 * <ul>
 * <li><em>public &lt;T&gt; T getTyped(String key)</em></li>
 * <li><em>public &lt;T&gt; T getTypedOr(String key, T defaultValue)</em></li>
 * </ul>
 * The first method lets the end user cast implicitly the returned Object into a custom type passed at call-time
 * <pre class="code"><code class="java">

 * // The old way
 * Map<String,Object> columns = manager.nativeQuery("SELECT * FROM users WHERE userId = 10").getFirst();

 * String name = (String)columns.get("name");  // BAD !
 * Long age = (Long)columns.get("age"); // BAD !

 * // With TypedMap
 * RegularStatement statement = session.newSimpleStatement("SELECT * FROM users WHERE userId = 10");
 * TypedMap columns = manager.typedQuery(statement).getOne();

 * // Explicit type (String) is passed to method invocation
 * String name = columns.&lt;String&gt;getTyped("name");

 * // No need to provide explicit type. The compiler will infer type in this case
 * Long age = columns.get("age");

 * </code></pre>

 * Since call to <em>getTyped(String key)</em> may return null, you can use the second method
 * <em>getTypedOr(String key, T defaultValue)</em> to pass a fallback value
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Custom-Types#typedmap" target="_blank">TypedMap</a>
 */
public class TypedMap extends LinkedHashMap<String, Object> {

    private static final long serialVersionUID = 1L;

    public static TypedMap of(String key, Object value) {
        final TypedMap typedMap = new TypedMap();
        typedMap.put(key, value);
        return typedMap;
    }

    public static TypedMap fromMap(Map<String, Object> source) {
        TypedMap typedMap = new TypedMap();
        typedMap.addAll(source);
        return typedMap;
    }

    @SuppressWarnings("unchecked")
    public <T> T getTyped(String key) {
        T value = null;
        if (super.containsKey(key) && super.get(key) != null) {
            value = (T) super.get(key);
            return value;
        }
        return value;
    }

    public <T> T getTypedOr(String key, T defaultValue) {
        if (super.containsKey(key)) {
            return getTyped(key);
        } else {
            return defaultValue;
        }
    }

    private void addAll(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            super.put(entry.getKey(), entry.getValue());
        }
    }
}
