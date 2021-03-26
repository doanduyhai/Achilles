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

import com.datastax.driver.core.ConsistencyLevel;

/**

 * Define the default consistency level for an Entity for all operations


 * <pre class="code"><code class="java">

 * {@literal @}Table(table = "user")
 * <strong>{@literal @}Consistency(read = ConsistencyLevel.ONE, write = ConsistencyLevel.ALL, serial = ConsistencyLevel.LOCAL_SERIAL)</strong>
 * public class User {...}

 * </code></pre></p>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#consistency" target="_blank">@Consistency</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface Consistency {
    /**
     * <strong>Mandatory</strong>. Consistency level for read operations
     */
    ConsistencyLevel read();

    /**
     * <strong>Mandatory</strong>. Consistency level for write operations
     */
    ConsistencyLevel write();

    /**
     * <strong>Mandatory</strong>. Consistency level for LightWeight Transaction operations
     */
    ConsistencyLevel serial();
}
