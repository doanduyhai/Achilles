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
 * Define the custom constructor to be use to instantiate the entity. There should be maximum
 * one non-default constructor annotated with @EntityCreator.
 * <br/>
 * <br/>
 * All the parameters of this custom constructor should:
 *
 * <ul>
 *     <li>1. have the same name as an existing mapped field</li>
 *     <li>2. have the same Java type as an existing mapped field (for primitive types, we rely on autoboxing so you can use 'Long' or 'long' for example, it does not matter)</li>
 * </ul>
 * <em>Please note that it is not mandatory to inject all mapped-fields in your custom constructor, some fields cat be set using setter or just let be null</em>
 * <br/>
 * <br/>
 * If you wish to use different parameter name rather than sticking to the field name, you can declare manually all the field names matching the parameters using the <em>fieldNames</em> attribute
 * <br/>
 * <br/>
 * Example:
 * <pre class="code"><code class="java">
 * {@literal @}Table
 * public class MyEntity {
 *
 *     {@literal @}PartitionKey
 *     private Long sensorId;
 *
 *     {@literal @}ClusteringColumn
 *     private Date date;
 *
 *     {@literal @}Column
 *     private Double value;
 *
 *     //Correct custom constructor with matching field name and type
 *     {@literal @}EntityCreator
 *     public MyEntity(Long sensorId, Date date, Double value) {
 *         this.sensorId = sensorId;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     //Correct custom constructor with matching field name and type even if fewer parameters than existing field name
 *     {@literal @}EntityCreator
 *     public MyEntity(Long sensorId, Date date) {
 *         this.sensorId = sensorId;
 *         this.date = date;
 *     }
 *
 *     //Correct custom constructor with matching field name and autoboxed type (long)
 *     {@literal @}EntityCreator
 *     public MyEntity(long sensorId, Date date, Double value) {
 *         this.sensorId = sensorId;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     //Correct custom constructor with declared field name and type
 *     {@literal @}EntityCreator({"sensorId", "date", "value"})
 *     public MyEntity(Long id, Date date, Double value) {
 *         this.sensorId = id;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     //Incorrect custom constructor because non matching field name (myId)
 *     {@literal @}EntityCreator
 *     public MyEntity(Long myId, Date date, Double value) {
 *         this.sensorId = myId;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     //Incorrect custom constructor because field name not found (sensor_id)
 *     {@literal @}EntityCreator({"sensor_id", "date", "value"})
 *     public MyEntity(Long sensor_id, Date date, Double value) {
 *         this.sensorId = sensor_id;
 *         this.date = date;
 *         this.value = value;
 *     }
 *
 *     //Incorrect custom constructor because all field names are not declared (missing declaration of "value" in annotation)
 *     {@literal @}EntityCreator({"sensorId", "date"})
 *     public MyEntity(Long sensor_id, Date date, Double value) {
 *         this.sensorId = sensor_id;
 *         this.date = date;
 *         this.value = value;
 *     }
 * }
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#entitycreator" target="_blank">@EntityCreator</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value=ElementType.CONSTRUCTOR)
@Documented
public @interface EntityCreator {

    String[] value() default {};
}
