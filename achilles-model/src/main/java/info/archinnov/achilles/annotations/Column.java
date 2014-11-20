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
 * <p>
 * Annotation for simple column. Examples:
 *
 * <pre class="code"><code class="java">
 *
 *   //Custom column name
 *   <strong>{@literal @}Column(name = "first_name")</strong>
 *   private String userFirstname;
 *
 *   //Static column
 *   <strong>{@literal @}Column(staticColumn = true)</strong>
 *   private String age;
 *
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#column" target="_blank">@Column</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Column {

	/**
	 * (<strong>Optional</strong>) The name of the column in <strong>CQL</strong>. Defaults to the field name.
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column(<strong>name = "user_name"</strong>)
     *   private String userFirstname;
     *
     * </code></pre>
     *
	 */
	String name() default "";

    /**
     * (<strong>Optional</strong>) Whether this column is a <strong>static</strong> column or not. Default = <strong>false</strong>
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}Column(<strong>staticColumn = true</strong>)
     *   private String userFirstname;
     *
     * </code></pre>
     *
     */
    boolean staticColumn() default false;
}
