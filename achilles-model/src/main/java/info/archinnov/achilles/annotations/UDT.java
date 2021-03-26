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
 * Annotation to indicate that a field is an <strong>UDT</strong> (User Defined Type). Examples:
 * <pre class="code"><code class="java">

 * //Inferred UDT name = "address"
 * <strong>{@literal @}UDT(keyspace = "test")</strong>
 * public class Address {...}
 * </code></pre>
 * <br/><br/>


 * <pre class="code"><code class="java">
 * //UDT name = "long_address"
 * <strong>{@literal @}UDT(keyspace = "test", name = "long_address")</strong>
 * public class Address {...};
 * </code></pre>
 * <br/><br/>

 * <pre class="code"><code class="java">

 * // Inferred UDT name = "my_address"
 * {@literal @}UDT(keyspace = "test")
 * <strong>{@literal @}Strategy(naming = NamingStrategy.SNAKE_CASE)</strong>
 * public class MyAddress {...};
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Entity-Mapping#user-defined-type" target="_blank">User Defined Type</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface UDT {

    /**
     * Provide the keyspace in which is defined the UDT
     */
    String keyspace() default "";

    /**
     * Force the UDT name
     */
    String name() default "";


}
