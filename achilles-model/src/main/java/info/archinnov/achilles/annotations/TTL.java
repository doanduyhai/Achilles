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
 * Define the default time to live, in <strong>second(s)</strong> for an Entity.
 * This time to live applies on all the fields on the entity
 * <pre class="code"><code class="java">

 * {@literal @}Table
 * <strong>{@literal @}TTL(3600)</strong>
 * public class Entity {...}
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#time-to-live" target="_blank">@TTL</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface TTL {

    /**
     * <strong>Mandatory</strong>: the default time to live value, expressed in <strong>second(s)</strong>

     * <pre class="code"><code class="java">

     * {@literal @}Table
     * <strong>{@literal @}TTL(3600)</strong>
     * public class Entity {...}
     * </code></pre>
     */
    int value();
}
