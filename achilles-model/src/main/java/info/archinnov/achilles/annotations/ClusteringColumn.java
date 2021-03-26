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
 * Indicates that this component is a clustering column key. Please note that the ordering <strong>starts at 1</strong>

 * <pre class="code"><code class="java">

 * {@literal @}Table
 * public class Entity {

 * // Partition key component
 * <strong>{@literal @}PartitionKey</strong>
 * private Long userId;

 * // Clustering column 1. Date in YYYYMMDD
 * <strong>{@literal @}ClusteringColumn(1)</strong>
 * private int date;

 * // Clustering column 2. type
 * <strong>{@literal @}ClusteringColumn(2)</strong>
 * private String type;
 * }

 * </code>
 * </pre>
 * </p>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#clusteringcolumn" target="_blank">@ClusteringColumn</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
public @interface ClusteringColumn {

    /**
     * The order of this clustering column, <strong>starting at 1</strong>
     *
     * @return
     */
    int value() default 1;

    /**
     * Define the <strong>clustering order</strong> on a clustering column. Default value is <strong>true</strong>


     * <pre class="code"><code class="java">
     * {@literal @}Table
     * public class Entity {

     * // Partition key
     * <strong>{@literal @}PartitionKey</strong>
     * private Long userId;

     * // Ascending clustering column. "asc" can be omitted because defaults to "true"
     * {@literal @}ClusteringColumn(value = 1, <strong>asc = true</strong>)
     * private UUID time;

     * // Descending clustering column.
     * {@literal @}ClusteringColumn(value = 2, <strong>asc = false</strong>)
     * private int type;
     * }

     * </code></pre>
     */
    boolean asc() default true;

}
