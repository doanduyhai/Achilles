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
 * Mark an entity/udt/view as immutable. The immutable entity should comply to the following rules
 *
 * <ol>
 *     <li>all fields should have <strong>public</strong> and <strong>final</strong> modifiers</li>
 *     <li>have no getter nor setter</li>
 *     <li>have exactly one non-default constructor that:
 *          <ul>
 *              <li>has as many argument as there are fields</li>
 *              <li>each argument should have the same type as its corresponding field (for primitive types, we rely on autoboxing so you can use 'Long' or 'long' for example, it does not matter)</li>
 *              <li>each argument name should match field name</li>
 *          </ul>
 *     </li>
 * </ol>
 *
 * Example of <strong>correct mapping</strong>:
 * <pre class="code"><code class="java">
 * {@literal @}Table
 * {@literal @}Immutable
 * public class MyImmutableEntity {
 *
 *     {@literal @}PartitionKey
 *     public final Long sensorId;
 *
 *     {@literal @}ClusteringColumn
 *     public final Date date;
 *
 *     {@literal @}Column
 *     public final Double value;
 *
 *     //Correct non-default constructor with matching field name and type
 *     public MyImmutableEntity(long sensorId, Date date, Double value) {
 *         this.sensorId = sensorId;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     // NO GETTER NOR SETTER !!!!!!!!!!!!!!!
 * }
 * </code></pre>
 *
 * Example of <strong>wrong mapping</strong> because constructor argument name does not match field name:
 * <pre class="code"><code class="java">
 * {@literal @}Table
 * {@literal @}Immutable
 * public class MyImmutableEntity {
 *
 *     {@literal @}PartitionKey
 *     public final Long sensorId;
 *
 *     {@literal @}ClusteringColumn
 *     public final Date date;
 *
 *     {@literal @}Column
 *     public final Double value;
 *
 *     //Incorrect, there is no field name "sensor_id" !!
 *     public MyImmutableEntity(long sensor_id, Date date, Double value) {
 *         this.sensorId = sensorId;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     // NO GETTER NOR SETTER !!!!!!!!!!!!!!!
 * }
 * </code></pre>
 *
 * Example of <strong>wrong mapping</strong> because constructor argument type does not match field type:
 * <pre class="code"><code class="java">
 * {@literal @}Table
 * {@literal @}Immutable
 * public class MyImmutableEntity {
 *
 *     {@literal @}PartitionKey
 *     public final Long sensorId;
 *
 *     {@literal @}ClusteringColumn
 *     public final Date date;
 *
 *     {@literal @}Column
 *     public final Double value;
 *
 *     //Incorrect, field sensorId is of type Long, not String !!
 *     public MyImmutableEntity(String sensor_id, Date date, Double value) {
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     // NO GETTER NOR SETTER !!!!!!!!!!!!!!!
 * }
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#immutable" target="_blank">@Immutable</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value=ElementType.TYPE)
@Documented
public @interface Immutable {

}
