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

import java.lang.annotation.*;

/**
 * Indicates that this component is a clustering column key. Please note that the ordering <strong>starts at 1</strong>
 *
 * <pre class="code"><code class="java">
 *
 *   <strong>{@literal @}EmbeddedId</strong>
 *   private CompoundKey compoundKey;
 *
 *   ...
 *
 *   public static class CompoundKey {
 *
 *      // Partition key component
 *      <strong>{@literal @}PartitionKey</strong>
 *      private Long userId;
 *
 *      // Clustering column 1. Date in YYYYMMDD
 *      <strong>{@literal @}ClusteringColumn(1)</strong>
 *      private int date;
 *
 *      // Clustering column 2. type
 *      <strong>{@literal @}ClusteringColumn(2)</strong>
 *      private String type;
 *   }
 *
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#clusteringcolumn" target="_blank">@ClusteringColumn</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Documented
public @interface ClusteringColumn {

    /**
     * The order of this clustering column, <strong>starting at 1</strong>
     * @return
     */
    int value() default 1;

    /**
     * <p>
     * Define the <strong>clustering order</strong> on a clustering key. This
     * attribute has <strong>no effect</strong> on a partition key component
     * </p>
     *
     * <pre class="code"><code class="java">
     *
     *   {@literal @}EmbeddedId
     *   private CompoundKey compoundKey;
     *
     *   ...
     *
     *   public static class CompoundKey {
     *
     *      // Partition key
     *      <strong>{@literal @}PartitionKey</strong>
     *      private Long userId;
     *
     *      // Clustering column
     *      {@literal @}ClusteringColumn(value = 1, <strong>reversed = true</strong>)
     *      private UUID time;
     *   }
     *
     * </code></pre>
     *
     */
    boolean reversed() default false;

}
