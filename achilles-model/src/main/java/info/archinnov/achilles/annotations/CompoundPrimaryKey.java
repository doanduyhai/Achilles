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
 * <p>
 * Indicates that a property is a compound primary key <br/>
 * The compound primary key class should contain properties annotated with @PartitionKey/@ClusteringColumn
 * <br/>
 * For compound primary keys having composite partition key, use the @PartitionKey
 * annotation many times on different columns
 * <br/>
 * <br/>
 *
 * Clustered entity with <strong>simple partition key</strong>
 * <pre class="code"><code class="java">
 *
 *   <strong>{@literal @}CompoundPrimaryKey</strong>
 *   private CompoundKey compoundKey;
 *
 *   ...
 *
 *   public static class CompoundKey {
 *
 *      // Partition key
 *      {@literal @}PartitionKey
 *      private Long userId;
 *
 *      // Clustering key
 *      {@literal @}ClusteringColumn
 *      private UUID time;
 *   }
 *
 * </code></pre>
 *
 * <br/>
 *
 * Entity with <strong>composite partition key</strong>
 * <pre class="code"><code class="java">
 *
 *   <strong>{@literal @}CompoundPrimaryKey</strong>
 *   private CompoundKey compoundKey;
 *
 *   ...
 *
 *   public static class CompoundKey {
 *
 *      // Partition key component 1
 *      <strong>{@literal @}PartitionKey(1)</strong>
 *      private Long userId;
 *
 *      // Partition key component 2. Date in YYYYMMDD
 *      <strong>{@literal @}PartitionKey(2)</strong>
 *      private int date;
 *   }
 *
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#compoundprimarykey" target="_blank">@CompoundPrimaryKey</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface CompoundPrimaryKey {

}
