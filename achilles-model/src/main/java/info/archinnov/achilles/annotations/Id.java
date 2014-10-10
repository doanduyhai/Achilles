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


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  <p>
 * Define a simple partition key for an entity
 *
 * <pre class="code"><code class="java">
 *
 *   <strong>{@literal @}Id</strong>
 *   private Long userId;
 *
 *   //Custom column name
 *   <strong>{@literal @}Id(name = "id")</strong>
 *   private Long userId;
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#id" target="_blank">@Id</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Id {
	/**
	 * (<strong>Optional</strong>) The name of the partition key. Defaults to the field name.
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Id(<strong>name = "user_id"</strong>)
     *   private Long userId;
     *
     * </code></pre>
     *
	 */
	String name() default "";
}
