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

import info.archinnov.achilles.type.strategy.InsertStrategy;
import info.archinnov.achilles.type.strategy.NamingStrategy;

/**

 * Choose a strategy for insertion and schema naming.<br/>

 * For insertion strategy, available values are :
 * <ul>
 * <li>{@code info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS}</li>
 * <li>{@code info.archinnov.achilles.type.InsertStrategy.NOT_NULL_FIELDS}</li>
 * </ul>
 * <br/>
 * Default value = {@code info.archinnov.achilles.type.InsertStrategy.ALL_FIELDS}

 * <pre class="code"><code class="java">

 * {@literal @}Table(table = "users")
 * <strong>{@literal @}Strategy(insert = InsertStrategy.NOT_NULL_FIELDS)</strong>
 * public class UserEntity;

 * </code></pre>
 * </p>

 * For naming strategy, available values are:

 * <ul>
 * <li>info.archinnov.achilles.type.NamingStrategy.SNAKE_CASE: transform all schema name using <a href="http://en.wikipedia.org/wiki/Snake_case" target="blank_">snake case</a></li>
 * <li>info.archinnov.achilles.type.NamingStrategy.CASE_SENSITIVE: enclose the name between double quotes (") for escaping the case</li>
 * <li>info.archinnov.achilles.type.NamingStrategy.LOWER_CASE: transform the name to lower case</li>
 * <li>info.archinnov.achilles.type.NamingStrategy.INHERIT_OR_LOWER_CASE: applies only to column name, either inherit the strategy from the class <strong>@Strategy</strong> annotation or default to lower case</li>
 * </ul>
 * <pre class="code"><code class="java">

 * {@literal @}Table(table = "usersTable")
 * <strong>{@literal @}Strategy(naming = NamingStrategy.SNAKE_CASE)</strong>
 * public class UserEntity;

 * </code></pre>
 *
 * @see <a href="http://github.com/doanduyhai/Achilles/wiki/Insert-Strategy" target="_blank">Achilles Insert Strategies</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface Strategy {

    /**
     * Strategy for insert
     * <br/>
     * Default = {@code info.archinnov.achilles.type.InsertStrategy.NOT_NULL_FIELDS}
     */
    InsertStrategy insert() default InsertStrategy.ALL_FIELDS;

    /**
     * Strategy for keyspace, table and column names.Available values are :
     * <ul>
     * <li>info.archinnov.achilles.type.NamingStrategy.SNAKE_CASE</li>
     * <li>info.archinnov.achilles.type.NamingStrategy.CASE_SENSITIVE</li>
     * <li>info.archinnov.achilles.type.NamingStrategy.LOWER_CASE.</li>
     * <li>info.archinnov.achilles.type.NamingStrategy.INHERIT_OR_LOWER_CASE.</li>
     * </ul>

     * If not set, defaults to {@code info.archinnov.achilles.type.NamingStrategy.LOWER_CASE}
     *
     * @return
     */
    NamingStrategy naming() default NamingStrategy.LOWER_CASE;
}
