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
 * Indicates that this component is part of the partition key.Useful to define a <strong>composite partition key</strong>
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
 *      // Partition key component 1
 *      {@literal @}Column
 *      {@literal @}Order(1)
 *      <strong>{@literal @}PartitionKey</strong>
 *      private Long userId;
 *
 *      // Partition key component 2. Date in YYYYMMDD
 *      {@literal @}Column
 *      {@literal @}Order(2)
 *      <strong>{@literal @}PartitionKey</strong>
 *      private int date;
 *   }
 *
 * </code></pre>
 * </p>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#partitionkey" target="_blank">@PartitionKey</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@Documented
public @interface PartitionKey {

}
