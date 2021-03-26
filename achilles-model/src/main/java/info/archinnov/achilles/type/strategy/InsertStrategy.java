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

package info.archinnov.achilles.type.strategy;

/**

 * Define a strategy for insertion. Available values are :
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
 *
 * @see <a href="http://github.com/doanduyhai/Achilles/wiki/Insert-Strategy" target="_blank">Achilles Insert Strategies</a>
 */
public enum InsertStrategy {
    ALL_FIELDS, NOT_NULL_FIELDS;
}
