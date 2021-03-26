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
 * Indicates that this component is part of the partition key.Useful to define a <strong>composite partition key</strong>.
 * Please note that the ordering <strong>starts at 1</strong>
 * <pre class="code"><code class="java">

 * {@literal @}Table
 * public class Entity {

 * // Partition key component 1
 * <strong>{@literal @}PartitionKey(1)</strong>
 * private Long id;

 * // Partition key component 2. Date in YYYYMMDD
 * <strong>{@literal @}PartitionKey(2)</strong>
 * private int date;
 * }
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#partitionkey" target="_blank">@PartitionKey</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
public @interface PartitionKey {

    /**
     * The order of this partition key, <strong>starting at 1</strong>
     */
    int value() default 1;

}
