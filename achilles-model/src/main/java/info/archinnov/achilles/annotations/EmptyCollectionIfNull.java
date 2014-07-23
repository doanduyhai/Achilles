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
 * In Cassandra there is no difference between an empty collection/map
 * and a null value for collection/map
 * <br/>
 * <br/>
 * In Java we do make the difference. This annotations allows mapping null values from
 * <strong>Cassandra</strong> into empty collection & map.
 * <br/>
 * <br/>
 * Empty list will default to <strong>ArrayList</strong>.<br/>
 * Empty set will default to <strong>HashSet</strong>.<br/>
 * Empty map will default to <strong>HashMap</strong>.<br/>
 *
 * <pre class="code"><code class="java">
 *
 *   {@literal @}Column
 *   <strong>{@literal @}EmptyCollectionIfNull</strong>
 *   private List<String> friends
 *
 * </code></pre>
 * </p>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Annotations#emptycollectionifnull" target="_blank">@EmptyCollectionIfNull</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface EmptyCollectionIfNull {

}
