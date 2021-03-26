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

 * Annotation for <strong>computed</strong> column. A <strong>computed</strong> column is a column obtained by
 * applying a CQL function on other(s) column(s). A <strong>computed</strong> column can only be referenced
 * in SELECT/read statements, not UPDATE/INSERT/DELETE
 * <br/>
 * Examples:

 * <pre class="code"><code class="java">

 * //Normal column name
 * {@literal @}Column(name = "first_name")
 * private String firstName;

 * //Write time of firstName
 * {@literal @}Column
 * <strong>{@literal @}Computed(function = "writetime", alias = "write_time", targetColumns = "first_name", cqlClass = Long.class)</strong>
 * private Long firstNameWriteTime;
 * </code></pre>

 * In the above example, the field <strong>firstNameWriteTime</strong> is obtained by applying the function
 * <strong>writime</strong> on the column <strong>first_name</strong>.
 * The target CQL class is <strong>Long</strong>.
 * <br/>
 * <strong>Please note that the targerFields should reference CQL column names, not Java field name. In the example
 * it should be "first_name" and not "firstName"</strong>
 * <br/><br/>
 * This annotation will generate the following SELECT statement at runtime:
 * <br/>
 * <pre class="code"><code class="java">
 * SELECT first_name, writetime(first_name) AS write_time ....
 * </code></pre>
 * </p>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#column" target="_blank">@Computed</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Computed {

    /**
     * <strong>Mandatory</strong>. The name of the <strong>CQL function</strong> to be used.
     */
    String function();

    /**
     * <strong>Mandatory</strong>. The alias of this computed column.
     * <strong>Warning: if you have multiple computed columns in the same entity, please take
     * care to choose different alias</strong>
     */
    String alias();

    /**
     * <strong>Mandatory</strong>. The target <string>CQL columns</string> on which the function will be applied.
     * You can passe in multiple columns. <strong>The ordering of columns does matter</strong>
     */
    String[] targetColumns();

    /**
     * <strong>Mandatory</strong>. To help Achilles determine the CQL type at compile time, you have to specify
     * the Cassandra Java data type produced by the function application. This type should be a CQL-compatible type.
     * <br>
     * Example: the <strong>writetime</strong> function will produce a CQL <string>bigint</string> so it's corresponding
     * Java type is <strong>Long</strong>
     */
    Class<?> cqlClass();
}
