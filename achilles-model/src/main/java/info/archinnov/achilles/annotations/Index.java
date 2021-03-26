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

 * Annotation for secondary index. Example
 * <pre class="code"><code class="java">

 * {@literal @}Table(table = "my_entity")
 * public class MyEntity {

 * //Simple index
 * {@literal @}Column
 * <strong>{@literal @}Index</strong>
 * private String countryCode;

 * //Simple index with custom name
 * {@literal @}Column("custom_name")
 * <strong>{@literal @}Index(name = "country_code_idx")</strong>
 * private String customName;

 * //Index on collection
 * {@literal @}Column
 * <strong>{@literal @}Index</strong>
 * private List&lt;String&gt; indexedList;

 * //Full index on collection because of the usage of <strong>{@literal @}Frozen</strong>
 * {@literal @}Column
 * <strong>{@literal @}Frozen</strong>
 * <strong>{@literal @}Index</strong>
 * private List&lt;String&gt; indexedFullList;

 * //Index on <strong>map key</strong>
 * {@literal @}Column("indexed_map_key")
 * private Map&lt;<strong>{@literal @}Index</strong> String, Long&gt; indexOnMapKey;

 * //Index on <strong>map entry</strong>
 * {@literal @}Column("index_map_entry")
 * <strong>{@literal @}Index</strong>
 * private Map&lt;String, Long&gt; indexOnMapEntry;

 * //Custom index
 * {@literal @}Column
 * <strong>{@literal @}Index(indexClassName = "com.myproject.SecondaryIndex", indexOptions = "{'key1': 'value1', 'key2': 'value2'}")</strong>
 * private String custom;

 * ...
 * }
 * </code></pre>

 * The above code would produce the following CQL script for index creation:
 * <pre class="code"><code class="sql">

 * CREATE INDEX IF NOT EXISTS ON my_entity(countryCode);

 * CREATE INDEX country_code_idx IF NOT EXISTS ON my_entity(custom_name);

 * CREATE INDEX IF NOT EXISTS ON my_entity(indexedlist);

 * CREATE INDEX IF NOT EXISTS ON my_entity(FULL(indexedfulllist));

 * CREATE INDEX IF NOT EXISTS ON my_entity(KEYS(indexed_map_key));

 * CREATE INDEX IF NOT EXISTS ON my_entity(ENTRY(index_map_entry));

 * CREATE CUSTOM my_entity_custom_index INDEX IF NOT EXISTS
 * ON my_entity(custom)
 * USING com.myproject.SecondaryIndex
 * WITH OPTIONS = {'key1': 'value1', 'key2': 'value2'};

 * </code></pre>

 * /<p>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#index" target="_blank">@Index</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE})
@Documented
public @interface Index {
    /**
    
     * <strong>Optional</strong>.
     * Define the name of the secondary index. If not set, defaults to <strong>table name_field name_index</strong>
     * <pre class="code"><code class="java">
     * {@literal @}Table
     * public class User {
    
     * ...
    
     * {@literal @}Index(<strong>name = "country_code_index"</strong>)
     * {@literal @}Column
     * private String countryCode;
    
     * ...
     * }
     * </code></pre>
     * </p>
    
     * If the index name was not set above, it would default to <string>user_countrycode_index</string>
     */
    String name() default "";

    /**
     * <strong>Optional</strong>. The class name of your custom secondary index implementation
     */
    String indexClassName() default "";

    /**
     * <strong>Optional</strong>. The options for your custom secondary index.
     * The indexOptions should be provided using the JSON map style. Example:
     * <br/><br/>
     * <strong>{'key1': 'property1', 'key2': 'property2', ...}</strong>
     */
    String indexOptions() default "";
}
