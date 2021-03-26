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
 * Annotation to makes <strong>Achilles</strong> serialize the object into JSON String. Examples:
 * <pre class="code"><code class="java">

 * {@literal @}Column
 * <strong>{@literal @}JSON</strong>
 * private MyObject myPojo;
 * </code></pre>

 * It could be used in <strong>nested</strong> collections too:

 * <pre class="code"><code class="java">

 * {@literal @}Column
 * private List&lt;<strong>{@literal @}JSON</strong> MyObject&gt; myPojos;
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Entity-Mapping#field-mapping" target="_blank">Field Mapping</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE_USE})
@Documented
public @interface JSON {
}
