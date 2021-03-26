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
 * Define a <strong>static</strong> column

 * <pre class="code"><code class="java">

 * {@literal @}Table
 * public class Entity {

 * // Partition key
 * {@literal @}PartitionKey
 * private Long id;

 * // Clustering column
 * {@literal @}ClusteringColumn
 * private int date;

 * // Static column
 * {@literal @}Column
 * <strong>{@literal @}Static</strong>
 * private String staticCol;
 * }
 * </code></pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Static {
}
