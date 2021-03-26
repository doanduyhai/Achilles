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

package info.archinnov.achilles.annotations;

import java.lang.annotation.*;

/**
 *
 * Marks a class as a function registry and let Achilles manage it
 * <pre class="code"><code class="java">
 * <strong>{@literal @}FunctionRegistry</strong>
 * public interface MyFunctions {
 *
 *      Integer sumOf(int val1, int val2);
 *
 *      Long toLong(Date javaDate);
 * }
 * </code></pre>
 * <br/>
 * <em>Note: it is possible to declare several function registries in your source code,
 * just annotate them with {@literal @}FunctionRegistry</em>
 * <br/><br/>
 * <strong>Warning: it is not possible to declare 2 different functions with the same name and signature in the same keyspace
 * Achilles will raise a compilation error when encountering such case.</strong> Ex:
 * <br/>
 * <br/>
 * <pre class="code"><code class="java">
 * <strong>{@literal @}FunctionRegistry</strong>
 * public interface MyFunctionRegistry {
 *
 *
 *      String toString(long value);
 *
 *      String toString(int value); // OK because parameter type is different
 *
 *      String toString(long value); // KO because same signature as the first function
 * }
 * </code></pre>
 *
 * <strong>Remark 1: functions return types cannot be primitive, use boxed types instead </strong>
 * <br/>
 * <br/>
 * <strong>Remark 2: Achilles' codec system also applies for function parameters and return type </strong>
 * <br/>
 * <br/>
 * <pre class="code"><code class="java">
 * {@literal @}FunctionRegistry
 * public interface FunctionsWithCodecSystemRegistry {
 *
 *      // CQL function signature = listtojson(consistencylevels list&lt;text&gt;), returns text
 *      String listToJson(List<<strong>@Enumerated</strong> ConsistencyLevel> consistencyLevels);
 *
 *      // CQL function signature = getinvalue(input text), returns text
 *      <strong>{@literal @}Codec(IntToString.class)</strong> String getIntValue(String input);
 *
 * }
 * </code></pre>
 * <strong>Remark 3: functions name and parameters' name are lower-cased by Cassandra automatically </strong>
 * <br/>
 * <br/>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Functions-And_Aggregates#function-registry" target="_blank">Function Registry</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface FunctionRegistry {


    /**
     * (<strong>Optional</strong>) The name of the keyspace in which the declared functions belong to.
     * If not set explicitly, <strong>Achilles</strong> will use the current
     * keyspace of the java driver <em>Session</em> object.
     * <br/>
     * <pre class="code"><code class="java">
     * <strong>{@literal @}FunctionRegistry(keyspace="production")</strong>
     * public class MyFunctions {...}
     * </code></pre>
     */
    String keyspace() default "";
}
