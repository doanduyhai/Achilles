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

package info.archinnov.achilles.annotations;

import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Define the consistency level for an Entity
 * For the PersistenceManager, the consistency level applies on all the fields on the entity
 *
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Entity(table = "user")
 *   <strong>{@literal @}Consistency(read = ConsistencyLevel.ONE, write = ConsistencyLevel.ALL)</strong>
 *   public class User
 *
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#consistency" target="_blank">@Consistency</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.FIELD })
@Documented
public @interface Consistency {
	/**
	 * <p>
	 * Consistency level for read operations. Default = <strong>ConsistencyLevel.ONE</strong>
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Entity(table = "user")
     *   {@literal @}Consistency(<strong>read = ConsistencyLevel.ONE</strong>)
     *   public class User
     *
     * </code></pre>
	 * </p>
	 */
	ConsistencyLevel read() default ConsistencyLevel.ONE;

	/**
	 * <p>
	 * Consistency level for write operations. Default = <strong>ConsistencyLevel.ONE</strong>
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Entity(table = "user")
     *   {@literal @}Consistency(<strong>write = ConsistencyLevel.QUORUM</strong>)
     *   public class User
     *
     * </code></pre>
     *
	 * </p>
	 */
	ConsistencyLevel write() default ConsistencyLevel.ONE;
}
