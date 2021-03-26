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

 * Annotation to indicate that a type is <strong>frozen</strong>. Examples:
 * <pre class="code"><code class="java">

 * <strong>{@literal @}UDT</strong>
 * public class Address{
 * {@literal @}Column
 * private String street;

 * {@literal @}Column
 * private int number;

 * {@literal @}Column
 * private int zipCode;

 * ...
 * }

 * {@literal @}Table
 * public class UserEntity {

 * ...

 * {@literal @}Column
 * <strong>{@literal @}Frozen</strong>
 * private Address address;
 * }

 * </code></pre>

 * It can be used in <strong>nested</strong> data types too:

 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private List&lt;<strong>{@literal @}Frozen</strong> Address&gt; addresses;
 * </code></pre>

 * Nested collections inside collections should be frozen too;

 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private Map&lt;Integer,<strong>{@literal @}Frozen</strong> List&lt;String&gt;&gt; nestedCollections;
 * </code></pre>
 * </p>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Entity-Mapping#user-defined-type" target="_blank">User Defined Type</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE})
@Documented
public @interface Frozen {

}
