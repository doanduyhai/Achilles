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

 * Transform a custom Java type into one of native types supported by the Java driver
 * <br/>

 * The Codec class provided should implement the {@link info.archinnov.achilles.type.codec.Codec} interface.
 * </p>
 * <br/>
 * <br/>
 * Let's consider the following codec transforming a <strong>Long</strong> to a <strong>String</strong>
 * <pre class="code"><code class="java">

 * public class LongToString implements Codec&lt;Long,String&gt; {
 * {@literal @}Override
 * public Class<Long> sourceType() {
 * return Long.class;
 * }

 * {@literal @}Override
 * public Class<String> targetType() {
 * return String.class;
 * }

 * {@literal @}Override
 * public String encode(Long fromJava) throws AchillesTranscodingException {
 * return fromJava.toString();
 * }

 * {@literal @}Override
 * public Long decode(String fromCassandra) throws AchillesTranscodingException {
 * return Long.parseLong(fromCassandra);
 * }
 * }
 * </code></pre>
 * <br/>
 * Example of <strong>simple Long</strong> type to <strong>String</strong> type transformation
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * <strong>{@literal @}Codec(LongToString.class)</strong>
 * private Long longToString;

 * </code></pre>
 * <br/>
 * Example of <strong>List&lt;Long&gt;</strong> to <strong>List&lt;String&gt;</strong> transformation
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private List&lt;<strong>{@literal @}Codec(LongToString.class)</strong> Long&gt; listOfLongToString;

 * </code></pre>

 * <br/>
 * Example of <strong>Set&lt;Long&gt;</strong> to <strong>Set&lt;String&gt;</strong> transformation
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private Set&lt;<strong>{@literal @}Codec(LongToString.class)</strong> Long&gt; setOfLongToString;

 * </code></pre>

 * <br/>
 * Example of key Map transformation: <strong>Map&lt;Long, Double&gt;</strong> to <strong>Map&lt;String, Double&gt;</strong>
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private Map&lt;<strong>{@literal @}Codec(LongToString.class)</strong> Long, Double&gt; mapKeyTransformation;

 * </code></pre>

 * <br/>
 * Example of value Map transformation: <strong>Map&lt;Integer, Long&gt;</strong> to <strong>Map&lt;Integer, String&gt;</strong>
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private Map&lt;Integer,<strong>{@literal @}Codec(LongToString.class)</strong>Long &gt; mapValueTransformation;

 * </code></pre>

 * <br/>
 * Example of key/value Map transformation: <strong>Map&lt;Long, Long&gt;</strong> to <strong>Map&lt;String, String&gt;</strong>
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private Map&lt;<strong>{@literal @}Codec(LongToString.class)</strong> Long, <strong>{@literal @}Codec(LongToString.class)</strong> Long &gt; mapKeyValueTransformation;
 * </code></pre>

 * <br/>
 * You can also have <strong>nested usage</strong> of {@literal @}Codec. The nesting level can be arbitrary and does not matter
 * <br/><br/>
 * 2-levels nesting
 * <pre class="code"><code class="java">
 * {@literal @}Column
 * private Tuple2&lt;String, Map&lt;Integer, <strong>{@literal @}Codec(LongToString.class)</strong> Long&gt;&gt; nested1Level
 * </code></pre>
 * <br/><br/>
 * 3-levels nesting
 * <pre class="code"><code class="java">
 * {@literal @}Column
 * private Tuple2&lt;String, Map&lt;Integer, List&lt;<strong>{@literal @}Codec(LongToString.class)</strong> Long&gt;&gt;&gt; nested2Level
 * </code></pre>
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE, ElementType.TYPE_PARAMETER})
@Documented
public @interface Codec {

    /**
     * Codec Implementation class. The provided Codec class should implement the {@link info.archinnov.achilles.type.codec.Codec} interface.
     */
    Class<? extends info.archinnov.achilles.type.codec.Codec> value();
}
