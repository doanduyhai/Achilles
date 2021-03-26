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

 * Marks a class as an entity and let Achilles manage it
 * <pre class="code"><code class="java">
 * <strong>{@literal @}Table</strong>
 * public class UserEntity {...}
 * </code></pre>

 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#entity" target="_blank">@Table</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface Table {

    /**
     * (<strong>Optional</strong>) The name of the keyspace in which this table belongs to.
     * If not set explicitly on the entity, <strong>Achilles</strong> will use the current
     * keyspace of the java driver <em>Session</em> object.
     * <br/>
     * <pre class="code"><code class="java">


     * <strong>{@literal @}Table(keyspace="production_data", table = "user")</strong>
     * public class UserEntity {...}
     * </code></pre>
     */
    String keyspace() default "";

    /**
     * (<strong>Optional</strong>) The name of the table. Defaults to the short class name. <br/>
     * Ex: for the class <em>xxx.xxx.UserEntity</em>, the
     * default table name will be <strong>userentity</strong> if the attribute <em>table</em> is not set.
     * <br/>
     * <pre class="code"><code class="java">

     * <strong>{@literal @}Table(table = "user")</strong>
     * public class UserEntity {...}
     * </code></pre>
     */
    String table() default "";
}
